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
package fr.aneo.armonik.client.model;

import com.google.gson.Gson;
import fr.aneo.armonik.client.definition.BlobDefinition;
import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.testutils.EventsGrpcMock;
import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.ResultsGrpcMock;
import fr.aneo.armonik.client.testutils.TasksGrpcMock;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_ABORTED;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_COMPLETED;
import static fr.aneo.armonik.client.model.TestDataFactory.blobHandle;
import static fr.aneo.armonik.client.model.TestDataFactory.sessionInfo;
import static fr.aneo.armonik.client.model.WorkerLibrary.*;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.*;

class SessionHandleTest extends InProcessGrpcTestBase {
  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private final TasksGrpcMock taskGrpcMock = new TasksGrpcMock();
  private final EventsGrpcMock eventsGrpcMock = new EventsGrpcMock();
  private BlobCompletionListenerMock outputTaskListener;
  private SessionInfo sessionInfo;
  private SessionHandle sessionHandle;

  @Override
  protected List<BindableService> services() {
    return List.of(resultsGrpcMock, taskGrpcMock, eventsGrpcMock);
  }

  @BeforeEach
  void setUp() {
    outputTaskListener = new BlobCompletionListenerMock();
    var sessionDefinition = new SessionDefinition(
      Set.of("partition_1"),
      new TaskConfiguration(3, 1, "partition_1", Duration.ofMinutes(60), Map.of("option1", "value1")),
      outputTaskListener,
      new BatchingPolicy(1, Duration.ofSeconds(1), 1, 1)
    );
    sessionInfo = sessionInfo("partition_1");
    sessionHandle = new SessionHandle(sessionInfo, sessionDefinition, channel);
    resultsGrpcMock.reset();
  }

  @Test
  @DisplayName("should submit a Task")
  void should_submit_a_task() {
    // Given
    var taskConfiguration = new TaskConfiguration(10, 4, "partition_1", Duration.ofMinutes(30), Map.of("option2", "value2"));
    var workerLibrary = new WorkerLibrary("my_library.jar", "fr.aneo.MyClass", blobHandle("sessionId", "libraryBlobId"));
    var taskDefinition = new TaskDefinition().withInput("name", BlobDefinition.from("John".getBytes()))
                                             .withOutput("result")
                                             .withWorkerLibrary(workerLibrary)
                                             .withConfiguration(taskConfiguration);

    // When
    var taskHandle = sessionHandle.submitTask(taskDefinition);
    eventsGrpcMock.emitStatusUpdate(idFrom(taskHandle.outputs().get("result")), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    sessionHandle.awaitOutputsProcessed();

    // Then
    validateTaskHandle(taskHandle, taskConfiguration);
    validateBlobAllocations();
    validateUploadedData(taskHandle);
    validateSubmittedSession();
    validateSubmittedDefaultTaskOptions();
    validateSubmittedTask(taskHandle, workerLibrary);
    validateSubmittedTaskOptions();
    validateTaskOutputListener(outputTaskListener);
  }

  @Test
  void should_await_outputs_processed_until_all_outputs_complete() throws Exception {
    // Given
    var awaitDone = new CountDownLatch(1);

    var taskDefinition1 = new TaskDefinition()
      .withInput("input1", BlobDefinition.from("data1".getBytes()))
      .withOutput("output1");

    var taskDefinition2 = new TaskDefinition()
      .withInput("input2", BlobDefinition.from("data2".getBytes()))
      .withOutput("output2");

    var task1 = sessionHandle.submitTask(taskDefinition1);
    var task2 = sessionHandle.submitTask(taskDefinition2);

    // When
    var awaitFuture = runAsync(() -> {
      sessionHandle.awaitOutputsProcessed();
      awaitDone.countDown();
    });

    // Then
    assertThat(awaitDone.await(150, MILLISECONDS)).isFalse();
    eventsGrpcMock.emitStatusUpdate(idFrom(task1.outputs().get("output1")), RESULT_STATUS_COMPLETED);

    // still be waiting after the first output completes
    assertThat(awaitDone.await(150, MILLISECONDS)).isFalse();

    eventsGrpcMock.emitStatusUpdate(idFrom(task2.outputs().get("output2")), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();

    // does not wait after the second output is finished
    assertThat(awaitDone.await(2, SECONDS)).isTrue();
    assertThatCode(() -> awaitFuture.get(200, MILLISECONDS)).doesNotThrowAnyException();

    assertThat(outputTaskListener.blobs).hasSize(2);
  }

  @Test
  void should_await_outputs_processed_with_some_failed_outputs() throws Exception {
    // Given
    var awaitDone = new CountDownLatch(1);

    var taskDefinition1 = new TaskDefinition()
      .withInput("input1", BlobDefinition.from("data1".getBytes()))
      .withOutput("output1");

    var taskDefinition2 = new TaskDefinition()
      .withInput("input2", BlobDefinition.from("data2".getBytes()))
      .withOutput("output2");

    var task1 = sessionHandle.submitTask(taskDefinition1);
    var task2 = sessionHandle.submitTask(taskDefinition2);

    // When
    var awaitFuture = runAsync(() -> {
      sessionHandle.awaitOutputsProcessed();
      awaitDone.countDown();
    });

    // Then
    assertThat(awaitDone.await(150, MILLISECONDS)).isFalse();
    eventsGrpcMock.emitStatusUpdate(idFrom(task1.outputs().get("output1")), RESULT_STATUS_ABORTED);

    // still be waiting after the first output completes
    assertThat(awaitDone.await(150, MILLISECONDS)).isFalse();

    eventsGrpcMock.emitStatusUpdate(idFrom(task2.outputs().get("output2")), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();

    // does not wait after the second output is finished
    assertThat(awaitDone.await(2, SECONDS)).isTrue();
    assertThatCode(() -> awaitFuture.get(200, MILLISECONDS)).doesNotThrowAnyException();

    assertThat(outputTaskListener.blobs).hasSize(1);
    assertThat(outputTaskListener.blobErrors).hasSize(1);
  }

  @Test
  void should_create_a_blob_handle() {
    // Given
    var blobDefinition = BlobDefinition.from("Hello World".getBytes());

    // when
    var blobHandle = sessionHandle.createBlob(blobDefinition);

    // then
    assertThat(blobHandle).isNotNull();
    assertThat(blobHandle.sessionId()).isEqualTo(sessionInfo.id());
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(1);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).sessionId).isEqualTo(sessionInfo.id().asString());
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).receivedData.toString()).isEqualTo("Hello World");
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).blobId).isEqualTo(blobHandle.deferredBlobInfo().toCompletableFuture().join().id().asString());
  }

  private void validateSubmittedSession() {
    assertThat(taskGrpcMock.submittedTasksRequest.getSessionId()).isEqualTo(sessionInfo.id().asString());
  }

  @SuppressWarnings("unchecked")
  private void validateUploadedData(TaskHandle taskHandle) {
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(2);

    // inputs
    assertThat(resultsGrpcMock.uploadedDataInfos)
      .filteredOn(info -> info.blobId.equals(idFrom(taskHandle.inputs().get("name")).asString()))
      .singleElement()
      .satisfies(info -> assertThat(info.receivedData.toString()).isEqualTo("John"));

    //payload
    assertThat(resultsGrpcMock.uploadedDataInfos)
      .filteredOn(info -> !info.blobId.equals(idFrom(taskHandle.inputs().get("name")).asString()))
      .singleElement()
      .satisfies(info -> {
          Map<String, Map<String, String>> json = new Gson().fromJson(info.receivedData.toString(), Map.class);
          assertThat(json).containsOnlyKeys("inputs", "outputs");
          assertThat(json.get("inputs")).containsExactly(entry("name", idFrom(taskHandle.inputs().get("name")).asString()));
          assertThat(json.get("outputs")).containsExactly(entry("result", idFrom(taskHandle.outputs().get("result")).asString()));
        }
      );
  }


  private void validateBlobAllocations() {
    assertThat(resultsGrpcMock.blobAllocationsCount).isEqualTo(3);
  }

  private void validateTaskHandle(TaskHandle taskHandle, TaskConfiguration taskConfiguration) {
    assertThat(taskHandle.sessionId()).isEqualTo(sessionInfo.id());
    assertThat(taskHandle.taskConfiguration()).isEqualTo(taskConfiguration);
    assertThat(taskHandle.deferredTaskInfo().toCompletableFuture().join().id()).isNotNull();
    assertThat(taskHandle.inputs()).containsOnlyKeys("name");
    assertThat(taskHandle.outputs()).containsOnlyKeys("result");
  }

  private void validateSubmittedTaskOptions() {
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).hasTaskOptions()).isTrue();
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getTaskOptions().getMaxRetries()).isEqualTo(10);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getTaskOptions().getPartitionId()).isEqualTo("partition_1");
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getTaskOptions().getMaxDuration().getSeconds()).isEqualTo(1_800);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getTaskOptions().getMaxDuration().getNanos()).isEqualTo(0);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getTaskOptions().getPriority()).isEqualTo(4);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getTaskOptions().getOptionsMap()).containsExactlyInAnyOrderEntriesOf(
      Map.of(
        "option2", "value2",
        CONVENTION_VERSION, "v1",
        LIBRARY_PATH, "my_library.jar",
        SYMBOL, "fr.aneo.MyClass",
        LIBRARY_BLOB_ID, "libraryBlobId"
      ));
  }

  private void validateSubmittedTask(TaskHandle taskHandle, WorkerLibrary workerLibrary) {
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreationsList()).hasSize(1);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getPayloadId()).isNotIn(
      idFrom(taskHandle.inputs().get("name")).asString(),
      idFrom(taskHandle.outputs().get("result")).asString()
    );
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getExpectedOutputKeysList())
      .containsExactly(idFrom(taskHandle.outputs().get("result")).asString());

    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreations(0).getDataDependenciesList()).containsExactly(
      idFrom(taskHandle.inputs().get("name")).asString(),
      idFrom(workerLibrary.libraryHandle()).asString());
  }

  private void validateSubmittedDefaultTaskOptions() {
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskOptions().getMaxRetries()).isEqualTo(3);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskOptions().getPartitionId()).isEqualTo("partition_1");
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskOptions().getMaxDuration().getSeconds()).isEqualTo(3_600);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskOptions().getMaxDuration().getNanos()).isEqualTo(0);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskOptions().getPriority()).isEqualTo(1);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskOptions().getOptionsMap()).isEqualTo(Map.of("option1", "value1"));
  }

  private void validateTaskOutputListener(BlobCompletionListenerMock outputTaskListener) {
    assertThat(outputTaskListener.blobs).hasSize(1);
  }

  private BlobId idFrom(BlobHandle handle) {
    return handle.deferredBlobInfo().toCompletableFuture().join().id();
  }

  static class BlobCompletionListenerMock implements BlobCompletionListener {
    List<Blob> blobs = new ArrayList<>();
    List<BlobError> blobErrors = new ArrayList<>();

    @Override
    public void onSuccess(Blob blob) {
      blobs.add(blob);
    }

    @Override
    public void onError(BlobError blobError) {
      blobErrors.add(blobError);
    }
  }
}
