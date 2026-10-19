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

import com.google.gson.Gson;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.client.definition.blob.OutputBlobDefinition;
import fr.aneo.armonik.client.testutils.*;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_ABORTED;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_COMPLETED;
import static fr.aneo.armonik.client.TestDataFactory.blobHandle;
import static fr.aneo.armonik.client.TestDataFactory.sessionInfo;
import static fr.aneo.armonik.client.WorkerLibrary.*;
import static fr.aneo.armonik.client.testutils.ResultsGrpcMock.MetadataRequest;
import static io.grpc.Status.UNAVAILABLE;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.*;

class SessionHandleTest extends InProcessGrpcTestBase {
  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private final TasksGrpcMock taskGrpcMock = new TasksGrpcMock();
  private final EventsGrpcMock eventsGrpcMock = new EventsGrpcMock();
  private final SessionsGrpcMock sessionsGrpcMock = new SessionsGrpcMock();
  private BlobCompletionListenerMock outputTaskListener;
  private SessionInfo sessionInfo;
  private SessionHandle sessionHandle;

  @Override
  protected List<BindableService> services() {
    return List.of(resultsGrpcMock, taskGrpcMock, eventsGrpcMock, sessionsGrpcMock);
  }

  @BeforeEach
  void setUp() {
    var taskConfiguration = new TaskConfiguration(3, 1, "partition_1", Duration.ofMinutes(60), Map.of("option1", "value1"));
    outputTaskListener = new BlobCompletionListenerMock();
    sessionInfo = sessionInfo("partition_1", taskConfiguration);
    sessionHandle = new SessionHandle(sessionInfo, outputTaskListener, channelPool);
    resultsGrpcMock.reset();
  }

  @Test
  @DisplayName("should submit a Task")
  void should_submit_a_task() {
    // Given
    var taskConfiguration = new TaskConfiguration(10, 4, "partition_1", Duration.ofMinutes(30), Map.of("option2", "value2"));
    var workerLibrary = new WorkerLibrary("my_library.jar", "fr.aneo.MyClass", blobHandle("sessionId", "libraryBlobId"));
    var taskDefinition = new TaskDefinition().withInput("name", InputBlobDefinition.from("John".getBytes()).withName("UserName"))
                                             .withOutput("result", OutputBlobDefinition.from("resultBlob", true))
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
  @DisplayName("should submit several tasks with one metadata call and one submission call")
  void should_submit_several_tasks_with_shared_calls() {
    // Given
    var definitions = IntStream.range(0, 3)
                               .mapToObj(i -> new TaskDefinition().withInput("data", InputBlobDefinition.from(("data" + i).getBytes()))
                                                                  .withOutput("result"))
                               .toList();

    // When
    var handles = sessionHandle.submitTasks(definitions);
    var taskInfos = handles.stream().map(handle -> handle.deferredTaskInfo().toCompletableFuture().join()).toList();

    // Then
    assertThat(resultsGrpcMock.metadataCalls).isEqualTo(1);
    assertThat(resultsGrpcMock.metadataRequests).hasSize(9);
    assertThat(taskGrpcMock.submitTasksRequests).hasSize(1);

    var taskCreations = taskGrpcMock.submittedTasksRequest.getTaskCreationsList();
    assertThat(taskCreations).hasSize(3);
    for (int i = 0; i < 3; i++) {
      assertThat(taskCreations.get(i).getExpectedOutputKeysList()).containsExactly(idFrom(handles.get(i).outputs().get("result")).asString());
      assertThat(taskCreations.get(i).getDataDependenciesList()).containsExactly(idFrom(handles.get(i).inputs().get("data")).asString());
      assertThat(taskInfos.get(i).id().asString()).isEqualTo("task-" + taskCreations.get(i).getPayloadId());
    }
  }

  @Test
  @DisplayName("should split large submissions into calls of at most 1000 tasks and 1000 blobs")
  void should_split_large_submissions() {
    // Given: 1001 tasks with a payload and one output each, so 2002 blobs
    var definitions = IntStream.range(0, TaskSubmitter.MAX_TASKS_PER_REQUEST + 1)
                               .mapToObj(i -> new TaskDefinition().withInput("data", blobHandle("sessionId", "data-id"))
                                                                  .withOutput("result"))
                               .toList();

    // When
    var handles = sessionHandle.submitTasks(definitions);
    handles.forEach(handle -> handle.deferredTaskInfo().toCompletableFuture().join());

    // Then
    assertThat(resultsGrpcMock.metadataCalls).isEqualTo(3);
    assertThat(taskGrpcMock.submitTasksRequests).extracting(request -> request.getTaskCreationsCount())
                                                .containsExactlyInAnyOrder(1000, 1);
  }

  @Test
  @DisplayName("should make no call when submitting an empty list")
  void should_make_no_call_for_empty_list() {
    assertThat(sessionHandle.submitTasks(List.of())).isEmpty();
    assertThat(resultsGrpcMock.metadataCalls).isZero();
    assertThat(taskGrpcMock.submitTasksRequests).isEmpty();
  }

  @Test
  @DisplayName("should reject a list containing null")
  void should_reject_list_containing_null() {
    var definitions = new ArrayList<TaskDefinition>();
    definitions.add(new TaskDefinition().withOutput("result"));
    definitions.add(null);

    assertThatThrownBy(() -> sessionHandle.submitTasks(definitions)).isInstanceOf(NullPointerException.class);
    assertThat(resultsGrpcMock.metadataCalls).isZero();
  }

  @Test
  @DisplayName("should await outputs processed until all outputs complete")
  void should_await_outputs_processed_until_all_outputs_complete() throws Exception {
    // Given
    var awaitDone = new CountDownLatch(1);

    var taskDefinition1 = new TaskDefinition()
      .withInput("input1", InputBlobDefinition.from("data1".getBytes()))
      .withOutput("output1");

    var taskDefinition2 = new TaskDefinition()
      .withInput("input2", InputBlobDefinition.from("data2".getBytes()))
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
  @DisplayName("should await outputs processed with some failed outputs")
  void should_await_outputs_processed_with_some_failed_outputs() throws Exception {
    // Given
    var awaitDone = new CountDownLatch(1);

    var taskDefinition1 = new TaskDefinition()
      .withInput("input1", InputBlobDefinition.from("data1".getBytes()))
      .withOutput("output1");

    var taskDefinition2 = new TaskDefinition()
      .withInput("input2", InputBlobDefinition.from("data2".getBytes()))
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
  @DisplayName("should report outputs as failed and stop waiting when the task submission fails")
  void should_report_outputs_as_failed_when_submission_fails() {
    // Given
    taskGrpcMock.submissionError = UNAVAILABLE.withDescription("Control plane is shutting down").asRuntimeException();
    var taskDefinition = new TaskDefinition().withInput("input", InputBlobDefinition.from("data".getBytes()))
                                             .withOutput("output1")
                                             .withOutput("output2");

    // When
    var task = sessionHandle.submitTask(taskDefinition);
    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> sessionHandle.awaitOutputsProcessed());

    // Then
    assertThat(task.deferredTaskInfo().toCompletableFuture()).isCompletedExceptionally();
    assertThat(outputTaskListener.blobs).isEmpty();
    assertThat(outputTaskListener.blobErrors)
      .extracting(blobError -> blobError.blobInfo().id())
      .containsExactlyInAnyOrder(idFrom(task.outputs().get("output1")), idFrom(task.outputs().get("output2")));
    assertThat(outputTaskListener.blobErrors)
      .allSatisfy(blobError -> assertThat(blobError.cause()).hasMessageContaining("Control plane is shutting down"));
  }

  @Test
  @DisplayName("should stop waiting when the output metadata creation fails")
  void should_stop_waiting_when_metadata_creation_fails() {
    // Given
    resultsGrpcMock.metadataError = UNAVAILABLE.asRuntimeException();
    var taskDefinition = new TaskDefinition().withInput("input", InputBlobDefinition.from("data".getBytes()))
                                             .withOutput("result");

    // When
    var task = sessionHandle.submitTask(taskDefinition);
    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> sessionHandle.awaitOutputsProcessed());

    // Then
    assertThat(task.deferredTaskInfo().toCompletableFuture()).isCompletedExceptionally();
    assertThat(task.outputs().get("result").deferredBlobInfo().toCompletableFuture()).isCompletedExceptionally();
    assertThat(taskGrpcMock.submitTasksRequests).isEmpty();
    assertThat(outputTaskListener.blobs).isEmpty();
    assertThat(outputTaskListener.blobErrors).isEmpty();
  }

  @Test
  @DisplayName("should not submit a task whose input upload fails, and report its outputs as failed")
  void should_not_submit_task_whose_input_upload_fails() {
    // Given
    resultsGrpcMock.failingUploadData = "bad".getBytes();
    var taskDefinition = new TaskDefinition().withInput("input", InputBlobDefinition.from("bad".getBytes()))
                                             .withOutput("result");

    // When
    var task = sessionHandle.submitTask(taskDefinition);
    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> sessionHandle.awaitOutputsProcessed());

    // Then
    assertThat(task.deferredTaskInfo().toCompletableFuture())
      .failsWithin(Duration.ZERO)
      .withThrowableThat()
      .withMessageContaining("Input 'input' of the task will never be available");
    assertThat(taskGrpcMock.submitTasksRequests).isEmpty();
    assertThat(outputTaskListener.blobErrors).extracting(blobError -> blobError.blobInfo().id())
                                             .containsExactly(idFrom(task.outputs().get("result")));
  }

  @Test
  @DisplayName("should not submit a task that depends on a task of the same batch whose input upload fails")
  void should_not_submit_consumer_of_failed_task_in_same_batch() throws Exception {
    // Given
    resultsGrpcMock.failingUploadData = "bad".getBytes();
    var producer = sessionHandle.submitTask(new TaskDefinition().withInput("input", InputBlobDefinition.from("bad".getBytes()))
                                                                .withOutput("result"));
    var consumer = sessionHandle.submitTask(new TaskDefinition().withInput("input", producer.outputs().get("result"))
                                                                .withOutput("result"));
    var independent = sessionHandle.submitTask(new TaskDefinition().withInput("input", InputBlobDefinition.from("good".getBytes()))
                                                                   .withOutput("result"));

    // When
    var awaitFuture = runAsync(() -> sessionHandle.awaitOutputsProcessed());
    independent.deferredTaskInfo().toCompletableFuture().get(5, SECONDS);
    eventsGrpcMock.emitStatusUpdate(idFrom(independent.outputs().get("result")), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    awaitFuture.get(5, SECONDS);

    // Then: the three tasks were in one batch, and only the independent one was sent
    assertThat(taskGrpcMock.submitTasksRequests).hasSize(1);
    assertThat(taskGrpcMock.submittedTasksRequest.getTaskCreationsCount()).isEqualTo(1);
    assertThat(producer.deferredTaskInfo().toCompletableFuture()).isCompletedExceptionally();
    assertThat(consumer.deferredTaskInfo().toCompletableFuture())
      .failsWithin(Duration.ZERO)
      .withThrowableThat()
      .withMessageContaining("Input 'input' of the task is an output of a task that was not submitted");
    assertThat(outputTaskListener.blobErrors).extracting(blobError -> blobError.blobInfo().id())
                                             .containsExactlyInAnyOrder(idFrom(producer.outputs().get("result")),
                                                                        idFrom(consumer.outputs().get("result")));
    assertThat(outputTaskListener.blobs).hasSize(1);
  }

  @Test
  @DisplayName("should not submit a task that depends on an output of an earlier task whose submission failed")
  void should_not_submit_consumer_of_task_whose_submission_failed() {
    // Given
    taskGrpcMock.submissionError = UNAVAILABLE.asRuntimeException();
    var producer = sessionHandle.submitTask(new TaskDefinition().withInput("input", InputBlobDefinition.from("data".getBytes()))
                                                                .withOutput("result"));
    sessionHandle.flushSubmissions().toCompletableFuture().join();
    assertThat(producer.deferredTaskInfo().toCompletableFuture()).isCompletedExceptionally();
    taskGrpcMock.submissionError = null;

    // When
    var consumer = sessionHandle.submitTask(new TaskDefinition().withInput("input", producer.outputs().get("result"))
                                                                .withOutput("result"));
    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> sessionHandle.awaitOutputsProcessed());

    // Then: only the producer's request reached the cluster
    assertThat(taskGrpcMock.submitTasksRequests).hasSize(1);
    assertThat(consumer.deferredTaskInfo().toCompletableFuture())
      .failsWithin(Duration.ZERO)
      .withThrowableThat()
      .withMessageContaining("Input 'input' of the task will never be available");
    assertThat(outputTaskListener.blobErrors).hasSize(2);
  }

  @Test
  @DisplayName("should not submit a task that uses a session blob whose upload fails")
  void should_not_submit_task_using_failed_session_blob() {
    // Given
    resultsGrpcMock.failingUploadData = "shared".getBytes();
    var shared = sessionHandle.createBlob(InputBlobDefinition.from("shared".getBytes()));

    // When
    var task = sessionHandle.submitTask(new TaskDefinition().withInput("shared", shared).withOutput("result"));
    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> sessionHandle.awaitOutputsProcessed());

    // Then
    assertThat(task.deferredTaskInfo().toCompletableFuture())
      .failsWithin(Duration.ZERO)
      .withThrowableThat()
      .withMessageContaining("Input 'shared' of the task will never be available");
    assertThat(taskGrpcMock.submitTasksRequests).isEmpty();
  }

  @Test
  @DisplayName("should create a blob handle")
  void should_create_a_blob_handle() throws InterruptedException, TimeoutException {
    // Given
    var blobDefinition = InputBlobDefinition.from("Hello World".getBytes());

    // when
    var blobHandle = sessionHandle.createBlob(blobDefinition);
    blobHandle.deferredBlobInfo().toCompletableFuture().join();
    resultsGrpcMock.awaitUploadsComplete(2, SECONDS);

    // then
    assertThat(blobHandle).isNotNull();
    assertThat(blobHandle.sessionId()).isEqualTo(sessionInfo.id());
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(1);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).sessionId).isEqualTo(sessionInfo.id().asString());
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).receivedData.toString()).isEqualTo("Hello World");
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).blobId).isEqualTo(blobHandle.deferredBlobInfo().toCompletableFuture().join().id().asString());
  }

  @Test
  @DisplayName("should cancel session with the session id")
  void should_cancel_session_with_the_session_id() {
    // When
    sessionHandle.cancel().toCompletableFuture().join();

    // Then
    assertThat(sessionsGrpcMock.submittedCancelSessionRequest).isNotNull();
    assertThat(sessionsGrpcMock.submittedCancelSessionRequest.getSessionId()).isEqualTo(sessionInfo.id().asString());
  }

  @Test
  @DisplayName("should pause session with the session id")
  void should_pause_session_with_the_session_id() {
    // When
    sessionHandle.pause().toCompletableFuture().join();

    // Then
    assertThat(sessionsGrpcMock.submittedPauseSessionRequest).isNotNull();
    assertThat(sessionsGrpcMock.submittedPauseSessionRequest.getSessionId()).isEqualTo(sessionInfo.id().asString());
  }

  @Test
  @DisplayName("should resume session with the session id")
  void should_resume_session_with_the_session_id() {
    // When
    sessionHandle.resume().toCompletableFuture().join();

    // Then
    assertThat(sessionsGrpcMock.submittedResumeSessionRequest).isNotNull();
    assertThat(sessionsGrpcMock.submittedResumeSessionRequest.getSessionId()).isEqualTo(sessionInfo.id().asString());
  }

  @Test
  @DisplayName("should close session with the session id")
  void should_close_session_with_the_session_id() {
    // When
    sessionHandle.close().toCompletableFuture().join();

    // Then
    assertThat(sessionsGrpcMock.submittedCloseSessionRequest).isNotNull();
    assertThat(sessionsGrpcMock.submittedCloseSessionRequest.getSessionId()).isEqualTo(sessionInfo.id().asString());
  }

  @Test
  @DisplayName("should purge session with the session id")
  void should_purge_session_with_the_session_id() {
    // When
    sessionHandle.purge().toCompletableFuture().join();

    // Then
    assertThat(sessionsGrpcMock.submittedPurgeSessionRequest).isNotNull();
    assertThat(sessionsGrpcMock.submittedPurgeSessionRequest.getSessionId()).isEqualTo(sessionInfo.id().asString());
  }

  @Test
  @DisplayName("should delete session with the session id")
  void should_delete_session_with_the_session_id() {
    // When
    sessionHandle.delete();

    // Then
    assertThat(sessionsGrpcMock.submittedDeleteSessionRequest).isNotNull();
    assertThat(sessionsGrpcMock.submittedDeleteSessionRequest.getSessionId()).isEqualTo(sessionInfo.id().asString());
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
    assertThat(resultsGrpcMock.metadataRequests).containsExactly(
      new MetadataRequest(sessionInfo.id().asString(), "payload", false),
      new MetadataRequest(sessionInfo.id().asString(), "UserName", false),
      new MetadataRequest(sessionInfo.id().asString(), "resultBlob", true)

    );
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
