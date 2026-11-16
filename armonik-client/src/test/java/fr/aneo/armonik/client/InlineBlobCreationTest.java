/*
 * Copyright © 2025 ANEO (armonik@aneo.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.aneo.armonik.client;

import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.ResultsGrpcMock;
import fr.aneo.armonik.client.testutils.SessionsGrpcMock;
import fr.aneo.armonik.client.testutils.TasksGrpcMock;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static fr.aneo.armonik.client.TestDataFactory.sessionInfo;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class InlineBlobCreationTest extends InProcessGrpcTestBase {
  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private final TasksGrpcMock tasksGrpcMock = new TasksGrpcMock();
  private final SessionsGrpcMock sessionsGrpcMock = new SessionsGrpcMock();
  private SessionInfo sessionInfo;

  @Override
  protected List<BindableService> services() {
    return List.of(resultsGrpcMock, tasksGrpcMock, sessionsGrpcMock);
  }

  @BeforeEach
  void setUp() {
    sessionInfo = sessionInfo("partition_1", new TaskConfiguration(3, 1, "partition_1", Duration.ofMinutes(60), Map.of()));
    resultsGrpcMock.reset();
  }

  @Test
  @DisplayName("should create small inputs and payloads with their data, without upload streams")
  void should_create_small_inputs_and_payloads_with_their_data() throws Exception {
    // Given
    resultsGrpcMock.dataChunkMaxSize = 1024 * 1024;
    var session = sessionHandle();

    // When
    var handles = session.submitTasks(IntStream.range(0, 3).mapToObj(i -> task("input-" + i)).toList());
    awaitSubmitted(handles);

    // Then: only the outputs go through metadata, nothing is streamed
    assertThat(resultsGrpcMock.uploadedDataInfos).isEmpty();
    assertThat(resultsGrpcMock.metadataRequests).hasSize(3);

    // And: the inputs and payloads carry their data, in two calls (inputs, then payloads)
    assertThat(resultsGrpcMock.createResultsRequests).hasSize(2);
    var taskCreations = tasksGrpcMock.submittedTasksRequest.getTaskCreationsList();
    assertThat(taskCreations).hasSize(3);
    for (int i = 0; i < 3; i++) {
      var taskCreation = taskCreations.get(i);
      var inputId = taskCreation.getDataDependencies(0);
      var outputId = taskCreation.getExpectedOutputKeys(0);
      assertThat(new String(resultsGrpcMock.createdData.get(inputId), UTF_8)).isEqualTo("input-" + i);
      var payload = new String(resultsGrpcMock.createdData.get(taskCreation.getPayloadId()), UTF_8);
      assertThat(payload).contains(inputId).contains(outputId);
    }
  }

  @Test
  @DisplayName("should keep each CreateResults request within DataChunkMaxSize")
  void should_keep_each_request_within_data_chunk_max_size() throws Exception {
    // Given: room for a few 100-byte inputs per request only
    resultsGrpcMock.dataChunkMaxSize = 600;
    var session = sessionHandle();
    var data = "x".repeat(100);

    // When
    var handles = session.submitTasks(IntStream.range(0, 10).mapToObj(i -> task(data)).toList());
    awaitSubmitted(handles);

    // Then: the mock rejects any larger request, and every task was submitted
    assertThat(resultsGrpcMock.createResultsRequests).hasSizeGreaterThan(2);
    assertThat(resultsGrpcMock.createResultsRequests).allSatisfy(request -> assertThat(request.getSerializedSize()).isLessThanOrEqualTo(600));
    assertThat(tasksGrpcMock.submitTasksRequests.stream().mapToInt(request -> request.getTaskCreationsCount()).sum()).isEqualTo(10);
    assertThat(resultsGrpcMock.uploadedDataInfos).isEmpty();
  }

  @Test
  @DisplayName("should stream an input too large for a request")
  void should_stream_an_input_too_large_for_a_request() throws Exception {
    // Given
    resultsGrpcMock.dataChunkMaxSize = 1000;
    var session = sessionHandle();
    var large = "y".repeat(5000);

    // When
    var handles = session.submitTasks(List.of(task(large)));
    awaitSubmitted(handles);

    // Then: the input gets metadata and an upload stream, the payload still goes inline
    assertThat(resultsGrpcMock.metadataRequests).hasSize(2);
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(1);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).receivedData.toString(UTF_8)).isEqualTo(large);
    assertThat(resultsGrpcMock.createResultsRequests).hasSize(1);
    assertThat(resultsGrpcMock.createResultsRequests.get(0).getResults(0).getName()).isEqualTo("payload");
  }

  @Test
  @DisplayName("should stream an input read from a file")
  void should_stream_an_input_read_from_a_file(@TempDir Path directory) throws Exception {
    // Given
    resultsGrpcMock.dataChunkMaxSize = 1024 * 1024;
    var file = Files.writeString(directory.resolve("input.bin"), "from a file");
    var session = sessionHandle();

    // When
    var handles = session.submitTasks(List.of(new TaskDefinition().withInput("A", InputBlobDefinition.from(file.toFile())).withOutput("result")));
    awaitSubmitted(handles);

    // Then
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(1);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).receivedData.toString(UTF_8)).isEqualTo("from a file");
  }

  @Test
  @DisplayName("should upload every blob as a stream when the service configuration is unavailable")
  void should_upload_every_blob_when_service_configuration_is_unavailable() throws Exception {
    // Given: GetServiceConfiguration is not implemented by the mock
    var session = sessionHandle();

    // When
    var handles = session.submitTasks(List.of(task("small")));
    awaitSubmitted(handles);

    // Then: one metadata call for payload, input and output, then two uploads
    assertThat(resultsGrpcMock.createResultsRequests).isEmpty();
    assertThat(resultsGrpcMock.metadataCalls).isEqualTo(1);
    assertThat(resultsGrpcMock.metadataRequests).extracting(ResultsGrpcMock.MetadataRequest::blobName).containsExactly("payload", "", "");
    resultsGrpcMock.awaitUploadsComplete(2, SECONDS);
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(2);
  }

  @Test
  @DisplayName("should not report a streamed input ready before its upload ends")
  void should_not_report_streamed_input_ready_before_upload() throws Exception {
    // Given: a streamed input whose upload fails
    resultsGrpcMock.dataChunkMaxSize = 1000;
    var large = "z".repeat(5000);
    resultsGrpcMock.failingUploadData = large.getBytes(UTF_8);
    // Long enough for the other task to be sent before the upload fails, were the input reported ready early
    resultsGrpcMock.failingUploadDelayMillis = 1000;
    var producer = sessionHandle();
    var consumer = sessionHandle();

    // When: the producing task waits in its session's submission buffer, and another task, in
    // another session, uses its input right away
    var first = producer.submitTask(task(large));
    var input = first.inputs().get("A");
    var second = consumer.submitTasks(List.of(new TaskDefinition().withInput("A", input).withOutput("result"))).get(0);

    // Then: neither task is submitted
    assertThat(second.deferredTaskInfo().toCompletableFuture())
      .failsWithin(5, SECONDS)
      .withThrowableOfType(java.util.concurrent.ExecutionException.class)
      .withMessageContaining("will never be available");
    assertThat(first.deferredTaskInfo().toCompletableFuture()).failsWithin(5, SECONDS).withThrowableOfType(java.util.concurrent.ExecutionException.class);
  }

  @Test
  @DisplayName("should fail the tasks when the pool closes before their payloads are created")
  void should_fail_tasks_when_pool_closes_before_payloads() throws Exception {
    // Given: a pool that refuses every call once the inputs and outputs are created
    resultsGrpcMock.dataChunkMaxSize = 1024 * 1024;
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    ChannelPool closing = new ChannelPool() {
      @Override
      public <T> T execute(java.util.function.Function<io.grpc.ManagedChannel, T> operation) {
        return channelPool.execute(operation);
      }

      @Override
      public <T> java.util.concurrent.CompletionStage<T> executeAsync(java.util.function.Function<io.grpc.ManagedChannel, java.util.concurrent.CompletionStage<T>> operation) {
        // GetServiceConfiguration, output metadata, inputs with data; then the payloads find the pool closed
        if (calls.incrementAndGet() > 3) throw new IllegalStateException("Channel pool has been shut down");
        return channelPool.executeAsync(operation);
      }

      @Override
      public void close() {
      }

      @Override
      public int size() {
        return channelPool.size();
      }

      @Override
      public int availableChannels() {
        return channelPool.availableChannels();
      }

      @Override
      public RetryPolicy retryPolicy() {
        return channelPool.retryPolicy();
      }
    };
    var session = new SessionHandle(sessionInfo, null, closing, TaskSubmissionBatching.DEFAULT, new ResultsServiceConfiguration(closing));

    // When
    var handle = session.submitTasks(List.of(task("small"))).get(0);

    // Then
    // A future that never completes would also pass failsWithin: require an actual failure
    assertThat(handle.deferredTaskInfo().toCompletableFuture()).failsWithin(5, SECONDS).withThrowableOfType(java.util.concurrent.ExecutionException.class);
    assertThat(calls.get()).as("calls made through the pool").isGreaterThan(3);
  }

  @Test
  @DisplayName("should read the service configuration once when the control plane does not implement it")
  void should_read_unimplemented_service_configuration_once() throws Exception {
    // Given: GetServiceConfiguration is not implemented by the mock
    var session = sessionHandle();

    // When
    awaitSubmitted(session.submitTasks(List.of(task("one"))));
    awaitSubmitted(session.submitTasks(List.of(task("two"))));

    // Then
    assertThat(resultsGrpcMock.serviceConfigurationCalls.get()).isEqualTo(1);
  }

  @Test
  @DisplayName("should retry a failed service configuration read only after the retry delay")
  void should_retry_failed_service_configuration_after_delay() throws Exception {
    // Given
    resultsGrpcMock.serviceConfigurationError = io.grpc.Status.UNAVAILABLE.asRuntimeException();
    var now = new java.util.concurrent.atomic.AtomicLong();
    var configuration = new ResultsServiceConfiguration(channelPool, now::get);

    // When / Then: the failure gives 0 and is not retried within the delay
    assertThat(configuration.dataChunkMaxSize().toCompletableFuture().get(5, SECONDS)).isZero();
    assertThat(configuration.dataChunkMaxSize().toCompletableFuture().get(5, SECONDS)).isZero();
    var callsBeforeDelay = resultsGrpcMock.serviceConfigurationCalls.get();

    // And: once the delay has passed and the service works, the size is read again
    resultsGrpcMock.serviceConfigurationError = null;
    resultsGrpcMock.dataChunkMaxSize = 4096;
    now.addAndGet(ResultsServiceConfiguration.RETRY_DELAY.toNanos() + 1);
    assertThat(configuration.dataChunkMaxSize().toCompletableFuture().get(5, SECONDS)).isEqualTo(4096);
    assertThat(resultsGrpcMock.serviceConfigurationCalls.get()).isEqualTo(callsBeforeDelay + 1);
  }

  private SessionHandle sessionHandle() {
    return new SessionHandle(sessionInfo, null, channelPool, TaskSubmissionBatching.DEFAULT, new ResultsServiceConfiguration(channelPool));
  }

  private static TaskDefinition task(String input) {
    return new TaskDefinition().withInput("A", InputBlobDefinition.from(input.getBytes(UTF_8))).withOutput("result");
  }

  private static void awaitSubmitted(List<TaskHandle> handles) throws Exception {
    for (var handle : handles) {
      handle.deferredTaskInfo().toCompletableFuture().get(5, SECONDS);
    }
  }
}
