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
package fr.aneo.armonik.worker;

import com.google.protobuf.Timestamp;
import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import fr.aneo.armonik.worker.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.definition.blob.OutputBlobDefinition;
import fr.aneo.armonik.worker.definition.task.TaskDefinition;
import fr.aneo.armonik.worker.testutils.AgentGrpcMock;
import fr.aneo.armonik.worker.testutils.InProcessGrpcTestBase;
import io.grpc.BindableService;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.*;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_CREATED;
import static io.grpc.Status.UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskContextTest extends InProcessGrpcTestBase {

  private final AgentGrpcMock agentGrpcMock = new AgentGrpcMock();

  @TempDir
  private Path tempDir;

  private BlobService blobService;
  private TaskContext taskContext;

  @BeforeEach
  void setUp() {
    var agentStub = AgentGrpc.newFutureStub(channel);
    var sessionId = SessionId.from("test-session");
    var communicationToken = "test-token";
    var agentNotifier = new AgentNotifier(agentStub, sessionId, communicationToken);
    var blobFileWriter = new BlobFileWriter(tempDir, agentNotifier);
    var taskService = new TaskService(agentStub, sessionId, communicationToken);
    blobService = new BlobService(agentStub, blobFileWriter, sessionId, communicationToken);
    taskContext = new TaskContext(
      blobService,
      taskService,
      sessionId,
      Map.of("input1", new TaskInput(BlobId.from("input1-id"), "input1", tempDir.resolve("input1-id"))),
      Map.of("output1", new TaskOutput(BlobId.from("output1-id"), "output1", blobFileWriter))
    );

    agentGrpcMock.reset();
  }

  @Override
  protected List<BindableService> services() {
    return List.of(agentGrpcMock);
  }

  @Test
  @DisplayName("Should throw an exception when input name is missing")
  void should_throw_exception_when_input_name_is_missing() {
    assertThatThrownBy(() -> taskContext.getInput("nonexistent")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("Should throw an exception when output name is missing")
  void should_throw_exception_when_output_name_is_missing() {
    assertThatThrownBy(() -> taskContext.getOutput("address")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should submit task with input and output definitions")
  void should_submit_task_with_input_and_output_definitions() {
    // Given
    var taskConfig = new TaskConfiguration(2, 5, "compute-partition", Duration.ofMinutes(10), Map.of("key", "value"));
    var taskDef = new TaskDefinition()
      .withInput("data1", InputBlobDefinition.from("inputData1", new byte[100]))
      .withInput("data2", InputBlobDefinition.from("inputData2", new byte[50]))
      .withOutput("result1", OutputBlobDefinition.from("resultData"))
      .withOutput("result2")
      .withConfiguration(taskConfig);

    // When
    var taskHandle = taskContext.submitTask(taskDef);

    // Then
    assertThat(taskHandle).isNotNull();
    assertThat(taskHandle.sessionId()).isEqualTo(SessionId.from("test-session"));
    assertThat(taskHandle.taskConfiguration()).isEqualTo(taskConfig);

    // Wait for async submission to complete
    var taskInfo = taskHandle.deferredTaskInfo().toCompletableFuture().join();
    assertThat(taskInfo).isNotNull();
    assertThat(taskInfo.id()).isNotNull();

    // Verify task was submitted to agent
    assertThat(agentGrpcMock.submittedTasks).isNotNull();
    assertThat(agentGrpcMock.submittedTasks.taskCreations()).hasSize(1);

    var taskCreation = agentGrpcMock.submittedTasks.taskCreations().get(0);
    assertThat(taskCreation.dataDependencies()).hasSize(2);
    assertThat(taskCreation.expectedOutputKeys()).hasSize(2);

    // Verify blobs were created
    assertThat(agentGrpcMock.uploadedBlobs).isNotNull();
    assertThat(agentGrpcMock.uploadedBlobs.blobs()).containsOnlyKeys("inputData1", "inputData2", "payload");
  }

  @Test
  @DisplayName("should submit task with existing input handles")
  void should_submit_task_with_existing_input_handles() {
    // Given
    var existingInputHandle = blobService.createBlobs(
      Map.of("existing", InputBlobDefinition.from("existingData", new byte[50]))).get("existing");

    var taskConfig = new TaskConfiguration(1, 1, "partition", Duration.ofMinutes(1), Map.of());
    var taskDef = new TaskDefinition()
      .withInput("newInput", InputBlobDefinition.from("newData", new byte[100]))
      .withInput("existingInput", existingInputHandle)
      .withOutput("result", OutputBlobDefinition.create())
      .withConfiguration(taskConfig);

    // When
    var taskHandle = taskContext.submitTask(taskDef);
    var taskInfo = taskHandle.deferredTaskInfo().toCompletableFuture().join();

    // Then
    assertThat(taskInfo).isNotNull();

    var taskCreation = agentGrpcMock.submittedTasks.taskCreations().get(0);
    assertThat(taskCreation.dataDependencies()).hasSize(2);
  }

  @Test
  @DisplayName("should wait for blob creation to complete before awaitCompletion returns")
  void should_wait_for_blob_creation_to_complete() throws Exception {
    // Given
    var delayedBlobCreation = new CompletableFuture<CreateResultsResponse>();
    agentGrpcMock.delayCreateResults(delayedBlobCreation);
    taskContext.submitTask(new TaskDefinition().withInput("slowBlob", InputBlobDefinition.from("slowBlob", new byte[100]))
                                               .withOutput("result"));

    // When
    var awaitCompletionFinished = new CountDownLatch(1);
    new Thread(() -> {
      taskContext.awaitCompletion();
      awaitCompletionFinished.countDown();
    }).start();

    // Then
    boolean finishedEarly = awaitCompletionFinished.await(200, TimeUnit.MILLISECONDS);
    assertThat(finishedEarly).isFalse();

    // When: Complete the blob creation
    delayedBlobCreation.complete(buildBlobCreationResponse());

    // Then: Should now complete within 2 seconds
    boolean finishedAfterCompletion = awaitCompletionFinished.await(2, TimeUnit.SECONDS);
    assertThat(finishedAfterCompletion).isTrue();
  }

  @Test
  @DisplayName("should wait for task submission to complete before awaitCompletion returns")
  void should_wait_for_task_submission_to_complete() throws Exception {
    // Given
    var delayedTaskSubmission = new CompletableFuture<SubmitTasksResponse>();
    agentGrpcMock.delaySubmitTasks(delayedTaskSubmission);
    taskContext.submitTask(new TaskDefinition().withInput("data", InputBlobDefinition.from("data", new byte[100]))
                                               .withOutput("result"));

    // When
    var awaitCompletionFinished = new CountDownLatch(1);
    new Thread(() -> {
      taskContext.awaitCompletion();
      awaitCompletionFinished.countDown();
    }).start();

    // Then: Should still be waiting after 200ms
    boolean finishedEarly = awaitCompletionFinished.await(200, TimeUnit.MILLISECONDS);
    assertThat(finishedEarly).isFalse();

    // When: Complete the task submission
    delayedTaskSubmission.complete(buildTaskSubmissionResponse());

    // Then: Should now complete within 2 seconds
    boolean finishedAfterCompletion = awaitCompletionFinished.await(2, TimeUnit.SECONDS);
    assertThat(finishedAfterCompletion).isTrue();
  }


  @Test
  @DisplayName("should throw ArmoniKException when task submission fails")
  void should_throw_when_task_submission_fails() {
    // Given: Configure mock to delay task submission
    var delayedTaskSubmission = new CompletableFuture<SubmitTasksResponse>();
    agentGrpcMock.delaySubmitTasks(delayedTaskSubmission);
    taskContext.submitTask(new TaskDefinition().withInput("data", InputBlobDefinition.from("data", new byte[100]))
                                               .withOutput("result"));

    // When
    var originalException = new StatusRuntimeException(UNAVAILABLE);
    delayedTaskSubmission.completeExceptionally(originalException);

    // Then: awaitCompletion should throw ArmoniKException wrapping the original error
    assertThatThrownBy(() -> taskContext.awaitCompletion())
      .isInstanceOf(ArmoniKException.class)
      .hasCause(originalException);
  }

  @Test
  @DisplayName("should create shared blob that can be reused in multiple tasks")
  void should_create_shared_blob_for_multiple_tasks() {
    // Given
    var sharedBlobDef = InputBlobDefinition.from("sharedData", new byte[100]);
    var sharedHandle = taskContext.createBlob(sharedBlobDef);

    // When
    var task1 = new TaskDefinition()
      .withInput("data", sharedHandle)
      .withOutput("result1");

    var task2 = new TaskDefinition()
      .withInput("data", sharedHandle)
      .withOutput("result2");

    taskContext.submitTask(task1);
    taskContext.submitTask(task2);
    taskContext.awaitCompletion();

    // Then
    assertThat(agentGrpcMock.uploadedBlobs.blobs()).containsKey("sharedData");
    assertThat(agentGrpcMock.submittedTasks.taskCreations()).hasSize(2);
    assertThat(agentGrpcMock.submittedTasks.taskCreations().get(0).dataDependencies()).contains("sharedData-id");
    assertThat(agentGrpcMock.submittedTasks.taskCreations().get(1).dataDependencies()).contains("sharedData-id");
  }

  private CreateResultsResponse buildBlobCreationResponse() {
    return CreateResultsResponse.newBuilder()
                                .setCommunicationToken("test-token")
                                .addResults(ResultMetaData.newBuilder()
                                                          .setResultId("slowBlobId")
                                                          .setSessionId("test-session")
                                                          .setStatus(RESULT_STATUS_CREATED)
                                                          .setCreatedAt(Timestamp.newBuilder()
                                                                                 .setSeconds(Instant.now().getEpochSecond())
                                                                                 .build())
                                                          .build())
                                .build();
  }

  private SubmitTasksResponse buildTaskSubmissionResponse() {
    return SubmitTasksResponse.newBuilder()
                              .addTaskInfos(SubmitTasksResponse.TaskInfo.newBuilder()
                                                                        .setTaskId("slowTaskId")
                                                                        .build())
                              .build();
  }
}
