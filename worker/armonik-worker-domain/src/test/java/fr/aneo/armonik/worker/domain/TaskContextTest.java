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
package fr.aneo.armonik.worker.domain;

import fr.aneo.armonik.worker.domain.definition.blob.BlobDefinition;
import fr.aneo.armonik.worker.domain.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.blob.OutputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.task.TaskDefinition;
import fr.aneo.armonik.worker.domain.internal.BlobService;
import fr.aneo.armonik.worker.domain.internal.BlobWriter;
import fr.aneo.armonik.worker.domain.internal.PayloadSerializer;
import fr.aneo.armonik.worker.domain.internal.TaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static fr.aneo.armonik.worker.domain.BlobStatus.CREATED;
import static fr.aneo.armonik.worker.domain.TestDataFactory.blobHandle;
import static java.time.Instant.now;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskContextTest {

  @TempDir
  private Path tempDir;

  private BlobServiceMock blobService;
  private TaskContext taskContext;
  private TaskServiceMock taskService;

  @BeforeEach
  void setUp() {
    var sessionId = SessionId.from("test-session");
    var blobWriter = mock(BlobWriter.class);
    var payloadSerializer = mock(PayloadSerializer.class);
    when(payloadSerializer.serialize(any())).thenReturn(new byte[0]);
    taskService = new TaskServiceMock();
    blobService = new BlobServiceMock();
    taskContext = new TaskContext(
      blobService,
      taskService,
      payloadSerializer,
      sessionId,
      Map.of("input1", new TaskInput(BlobId.from("input1-id"), "input1", tempDir.resolve("input1-id"))),
      Map.of("output1", new TaskOutput(BlobId.from("output1-id"), "output1", blobWriter))
    );
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
    var existing = blobHandle("test-session", "existing-id");
    var parentInput = taskContext.getInput("input1");
    var parentOutput = taskContext.getOutput("output1");
    var inputData1Definition = InputBlobDefinition.from("inputData1", new byte[100]);
    var resultDefinition = OutputBlobDefinition.from("resultData");
    var taskDef = new TaskDefinition().withConfiguration(taskConfig)
                                      .withInput("inputDefinition", inputData1Definition)
                                      .withInput("existingBlobHandle", existing)
                                      .withInput("parentInput", parentInput)
                                      .withOutput("result")
                                      .withOutput("resultDefinition", resultDefinition)
                                      .withOutput("parentOutput", parentOutput);
    // When
    var taskHandle = taskContext.submitTask(taskDef);

    // Then
    assertThat(blobService.createBlobsRequest).hasSize(1);
    assertThat(blobService.createBlobsRequest.get(0)).containsExactlyInAnyOrderEntriesOf(Map.of("inputDefinition", inputData1Definition));

    assertThat(blobService.createBlobRequest).hasSize(1); // payload
    assertThat(blobService.createBlobRequest.get(0)).isNotNull(); // payload

    assertThat(blobService.prepareBlobsRequest).hasSize(1);
    assertThat(blobService.prepareBlobsRequest.get(0)).hasSize(2);
    assertThat(blobService.prepareBlobsRequest.get(0).keySet()).containsExactlyInAnyOrder("result", "resultDefinition");
    assertThat(blobService.prepareBlobsRequest.get(0).get("resultDefinition")).isEqualTo(resultDefinition);
    assertThat(blobService.prepareBlobsRequest.get(0).get("result")).isNotNull();

    assertThat(taskService.submissions).hasSize(1);
    assertThat(taskService.submissions.get(0).inputIds).containsExactlyInAnyOrderElementsOf(List.of(
      blobService.createBlobIdsResponse.get(0).get(0),
      BlobId.from("existing-id"),
      BlobId.from("input1-id"))
    );
    assertThat(taskService.submissions.get(0).outputIds).containsExactlyInAnyOrderElementsOf(List.of(
      blobService.prepareBlobIdsResponse.get(0).get(0),
      blobService.prepareBlobIdsResponse.get(0).get(1),
      BlobId.from("output1-id")

    ));
    assertThat(taskService.submissions.get(0).payloadId).isEqualTo(blobService.createBlobIdResponse.get(0));
    assertThat(taskService.submissions.get(0).taskConfiguration()).isEqualTo(taskConfig);

    assertThat(taskHandle).isNotNull();
    assertThat(taskHandle.sessionId()).isEqualTo(SessionId.from("test-session"));
    assertThat(taskHandle.taskConfiguration()).isEqualTo(taskConfig);
  }


  @Test
  @DisplayName("should wait for blob creation to complete before awaitCompletion returns")
  void should_wait_for_blob_creation_to_complete() throws Exception {
    // Given
    var delayedBlobInfo = new CompletableFuture<BlobInfo>();
    blobService.delayedBlobInfo = delayedBlobInfo;
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
    delayedBlobInfo.complete(new BlobInfo(BlobId.from("slowBlob-id"), CREATED, now()));

    // Then: Should now complete within 2 seconds
    boolean finishedAfterCompletion = awaitCompletionFinished.await(2, TimeUnit.SECONDS);
    assertThat(finishedAfterCompletion).isTrue();
  }

  //
  @Test
  @DisplayName("should wait for task submission to complete before awaitCompletion returns")
  void should_wait_for_task_submission_to_complete() throws Exception {
    // Given
    var delayedTaskInfo = new CompletableFuture<TaskInfo>();
    taskService.delayedTaskInfo = delayedTaskInfo;
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
    delayedTaskInfo.complete(new TaskInfo(TaskId.from("taskId")));

    // Then: Should now complete within 2 seconds
    boolean finishedAfterCompletion = awaitCompletionFinished.await(2, TimeUnit.SECONDS);
    assertThat(finishedAfterCompletion).isTrue();
  }


  @Test
  @DisplayName("should throw ArmoniKException when task submission fails")
  void should_throw_when_task_submission_fails() {
    // Given: Configure mock to delay task submission
    var delayedTaskSubmission = new CompletableFuture<TaskInfo>();
    taskService.delayedTaskInfo = delayedTaskSubmission;
    taskContext.submitTask(new TaskDefinition().withInput("data", InputBlobDefinition.from("data", new byte[100]))
                                               .withOutput("result"));

    // When
    var originalException = new RuntimeException("Boom");
    delayedTaskSubmission.completeExceptionally(originalException);

    // Then: awaitCompletion should throw ArmoniKException wrapping the original error
    assertThatThrownBy(() -> taskContext.awaitCompletion()).isInstanceOf(ArmoniKException.class);
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
    assertThat(blobService.createBlobRequest).filteredOn(definition -> definition == sharedBlobDef).hasSize(1);
    assertThat(taskService.submissions).hasSize(2);
    assertThat(taskService.submissions.get(0).inputIds).contains(sharedHandle.deferredBlobInfo().toCompletableFuture().join().id());
    assertThat(taskService.submissions.get(1).inputIds).contains(sharedHandle.deferredBlobInfo().toCompletableFuture().join().id());
  }

  static class BlobServiceMock implements BlobService {
    List<Map<String, InputBlobDefinition>> createBlobsRequest = new ArrayList<>();
    List<Map<String, BlobHandle>> createBlobsResponse = new ArrayList<>();
    List<List<BlobId>> createBlobIdsResponse = new ArrayList<>();

    List<InputBlobDefinition> createBlobRequest = new ArrayList<>();
    List<BlobHandle> createBlobResponse = new ArrayList<>();
    List<BlobId> createBlobIdResponse = new ArrayList<>();

    List<Map<String, BlobDefinition>> prepareBlobsRequest = new ArrayList<>();
    List<Map<String, BlobHandle>> prepareBlobsResponse = new ArrayList<>();
    List<List<BlobId>> prepareBlobIdsResponse = new ArrayList<>();

    CompletableFuture<BlobInfo> delayedBlobInfo;

    @Override
    public Map<String, BlobHandle> createBlobs(Map<String, InputBlobDefinition> inputBlobDefinitions) {
      createBlobsRequest.add(inputBlobDefinitions);
      var createBlobResponse = inputBlobDefinitions.entrySet()
                                                   .stream()
                                                   .collect(toMap(
                                                     Map.Entry::getKey,
                                                     entry -> {
                                                       if (delayedBlobInfo != null && entry.getKey().equals("slowBlob")) {
                                                         return new BlobHandle(SessionId.from("test-session"), entry.getKey(), delayedBlobInfo);
                                                       } else {
                                                         return blobHandle("test-session", entry.getKey() + "-id");
                                                       }
                                                     }));
      createBlobsResponse.add(createBlobResponse);
      if (delayedBlobInfo == null) {
        createBlobIdsResponse.add(createBlobResponse.values().stream()
                                                    .map(handle -> handle.deferredBlobInfo().toCompletableFuture().join().id())
                                                    .toList());
      } else {
        createBlobIdsResponse = List.of();
      }
      return createBlobResponse;
    }

    @Override
    public BlobHandle createBlob(InputBlobDefinition inputBlobDefinition) {
      createBlobRequest.add(inputBlobDefinition);
      var blobHandle = blobHandle("test-session", "singleBlob-id");
      createBlobResponse.add(blobHandle);
      createBlobIdResponse.add(blobHandle.deferredBlobInfo().toCompletableFuture().join().id());

      return blobHandle;
    }

    @Override
    public <T extends BlobDefinition> Map<String, BlobHandle> prepareBlobs(Map<String, T> blobDefinitions) {
      prepareBlobsRequest.add((Map<String, BlobDefinition>) blobDefinitions);
      var prepareBlobResponse = blobDefinitions.entrySet()
                                               .stream()
                                               .collect(toMap(
                                                 Map.Entry::getKey,
                                                 entry -> blobHandle("test-session", entry.getKey() + "-id")));
      prepareBlobsResponse.add(prepareBlobResponse);
      prepareBlobIdsResponse.add(prepareBlobResponse.values().stream().map(blobHandle -> blobHandle.deferredBlobInfo().toCompletableFuture().join().id()).toList());
      return prepareBlobResponse;
    }
  }

  static class TaskServiceMock implements TaskService {
    List<TaskSubmission> submissions = new ArrayList<>();

    CompletableFuture<TaskInfo> delayedTaskInfo;

    @Override
    public CompletionStage<TaskInfo> submitTask(Collection<BlobId> inputIds, Collection<BlobId> outputIds, BlobId payloadId, TaskConfiguration taskConfiguration) {
      submissions.add(new TaskSubmission(inputIds, outputIds, payloadId, taskConfiguration));
      return delayedTaskInfo != null ? delayedTaskInfo : completedFuture(new TaskInfo(TaskId.from("task-id")));
    }
  }

  record TaskSubmission(Collection<BlobId> inputIds,
                        Collection<BlobId> outputIds,
                        BlobId payloadId,
                        TaskConfiguration taskConfiguration) {
  }
}

