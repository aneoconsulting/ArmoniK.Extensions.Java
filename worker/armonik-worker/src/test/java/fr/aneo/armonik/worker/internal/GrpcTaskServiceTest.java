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
package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import fr.aneo.armonik.worker.domain.BlobId;
import fr.aneo.armonik.worker.domain.SessionId;
import fr.aneo.armonik.worker.domain.TaskConfiguration;
import fr.aneo.armonik.worker.domain.WorkerLibrary;
import fr.aneo.armonik.worker.domain.internal.TaskSubmission;
import fr.aneo.armonik.worker.testutils.AgentGrpcMock;
import fr.aneo.armonik.worker.testutils.InProcessGrpcTestBase;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcTaskServiceTest extends InProcessGrpcTestBase {

  private final AgentGrpcMock agentGrpcMock = new AgentGrpcMock();
  private GrpcTaskService taskService;

  @BeforeEach
  void setUp() {
    var agentStub = AgentGrpc.newFutureStub(channel);
    taskService = new GrpcTaskService(agentStub, SessionId.from("test-session"), "test-communication-token");
    agentGrpcMock.reset();
  }

  @Override
  protected List<BindableService> services() {
    return List.of(agentGrpcMock);
  }

  @Test
  @DisplayName("should submit task with input blobs, output blobs and payload")
  void should_submit_task_with_inputs_outputs_and_payload() {
    // Given
    var inputIds = List.of(BlobId.from("input1-id"), BlobId.from("input2-id"));
    var outputIds = List.of(BlobId.from("output1-id"), BlobId.from("output2-id"));
    var payloadId = BlobId.from("payload-id");
    var taskConfig = new TaskConfiguration(2, 5, "custom-partition", Duration.ofMinutes(10), Map.of("custom-key", "custom-value"));

    // When
    var taskInfo = taskService.submitTask(inputIds, outputIds, payloadId, taskConfig).toCompletableFuture().join();

    // Then
    assertThat(taskInfo).isNotNull();
    assertThat(taskInfo.id()).isNotNull();

    assertThat(agentGrpcMock.submittedTasks).isNotNull();
    assertThat(agentGrpcMock.submittedTasks.sessionId()).isEqualTo("test-session");
    assertThat(agentGrpcMock.submittedTasks.communicationToken()).isEqualTo("test-communication-token");
    assertThat(agentGrpcMock.submittedTasks.taskCreations()).hasSize(1);

    var taskCreation = agentGrpcMock.submittedTasks.taskCreations().get(0);
    assertThat(taskCreation.payloadId()).isEqualTo("payload-id");
    assertThat(taskCreation.dataDependencies()).containsExactlyInAnyOrder("input1-id", "input2-id");
    assertThat(taskCreation.expectedOutputKeys()).containsExactlyInAnyOrder("output1-id", "output2-id");
    assertThat(taskCreation.taskOptions()).isNotNull();
    assertThat(taskCreation.taskOptions().getMaxDuration().getSeconds()).isEqualTo(600);
    assertThat(taskCreation.taskOptions().getMaxDuration().getNanos()).isEqualTo(0);
    assertThat(taskCreation.taskOptions().getPriority()).isEqualTo(5);
    assertThat(taskCreation.taskOptions().getMaxRetries()).isEqualTo(2);
    assertThat(taskCreation.taskOptions().getPartitionId()).isEqualTo("custom-partition");
    assertThat(taskCreation.taskOptions().getOptionsMap()).containsEntry("custom-key", "custom-value");
  }

  @Test
  @DisplayName("should add the worker library blob to the data dependencies")
  void should_add_worker_library_blob_to_data_dependencies() {
    // Given
    var inputIds = List.of(BlobId.from("input1-id"));
    var outputIds = List.of(BlobId.from("output1-id"));
    var taskConfig = new TaskConfiguration(2, 5, "dynamic", Duration.ofMinutes(10), Map.of(
      WorkerLibrary.LIBRARY_BLOB_ID, "library-id",
      WorkerLibrary.LIBRARY_PATH, "lib/processor.jar",
      WorkerLibrary.SYMBOL, "Processor",
      WorkerLibrary.CONVENTION_VERSION, "v1"
    ));

    // When
    taskService.submitTask(inputIds, outputIds, BlobId.from("payload-id"), taskConfig).toCompletableFuture().join();

    // Then
    var taskCreation = agentGrpcMock.submittedTasks.taskCreations().get(0);
    assertThat(taskCreation.dataDependencies()).containsExactly("input1-id", "library-id");
    assertThat(taskCreation.taskOptions().getOptionsMap()).containsEntry(WorkerLibrary.LIBRARY_BLOB_ID, "library-id");
  }

  @Test
  @DisplayName("should not duplicate the worker library blob already in the inputs")
  void should_not_duplicate_worker_library_blob_already_in_inputs() {
    // Given
    var inputIds = List.of(BlobId.from("library-id"));
    var outputIds = List.of(BlobId.from("output1-id"));
    var taskConfig = new TaskConfiguration(2, 5, "dynamic", Duration.ofMinutes(10), Map.of(WorkerLibrary.LIBRARY_BLOB_ID, "library-id"));

    // When
    taskService.submitTask(inputIds, outputIds, BlobId.from("payload-id"), taskConfig).toCompletableFuture().join();

    // Then
    assertThat(agentGrpcMock.submittedTasks.taskCreations().get(0).dataDependencies()).containsExactly("library-id");
  }

  @Test
  @DisplayName("should submit several tasks in one request and return task infos in order")
  void should_submit_several_tasks_in_one_request() {
    // Given
    var submissions = IntStream.range(0, 3).mapToObj(GrpcTaskServiceTest::submission).toList();

    // When
    var taskInfos = taskService.submitTasks(submissions).stream().map(stage -> stage.toCompletableFuture().join()).toList();

    // Then
    assertThat(agentGrpcMock.submitTasksCalls).isEqualTo(1);
    assertThat(agentGrpcMock.submittedTasks.taskCreations()).extracting(AgentGrpcMock.CapturedTaskCreation::payloadId)
                                                           .containsExactly("payload0", "payload1", "payload2");
    assertThat(taskInfos).extracting(taskInfo -> taskInfo.id().asString())
                         .containsExactly("task-payload0", "task-payload1", "task-payload2");
  }

  @Test
  @DisplayName("should split submissions into requests of MAX_TASKS_PER_REQUEST tasks")
  void should_split_submissions_into_groups() {
    // Given
    var submissions = IntStream.range(0, GrpcTaskService.MAX_TASKS_PER_REQUEST + 1).mapToObj(GrpcTaskServiceTest::submission).toList();

    // When
    var stages = taskService.submitTasks(submissions);

    // Then
    assertThat(stages).hasSize(GrpcTaskService.MAX_TASKS_PER_REQUEST + 1);
    assertThat(stages.get(GrpcTaskService.MAX_TASKS_PER_REQUEST).toCompletableFuture().join().id().asString()).isEqualTo("task-payload1000");
    assertThat(agentGrpcMock.submitTasksCalls).isEqualTo(2);
  }

  @Test
  @DisplayName("should split submissions so that no request exceeds the Agent size limit")
  void should_split_submissions_by_request_size() {
    // Given: 1000 reduce tasks over 1500 inputs each, about 60 KB of ids per task
    var inputIds = IntStream.range(0, 1500).mapToObj(i -> BlobId.from(UUID.randomUUID().toString())).toList();
    var submissions = IntStream.range(0, 1000)
                               .mapToObj(i -> new TaskSubmission(inputIds, List.of(BlobId.from("out" + i)), BlobId.from("payload" + i), null))
                               .toList();

    // When
    var taskInfos = taskService.submitTasks(submissions).stream().map(stage -> stage.toCompletableFuture().join()).toList();

    // Then
    assertThat(agentGrpcMock.submitTasksCalls).isGreaterThan(1);
    assertThat(agentGrpcMock.requestSizes).allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(AgentRequests.MAX_REQUEST_SIZE));
    assertThat(taskInfos.get(999).id().asString()).isEqualTo("task-payload999");
  }

  @Test
  @DisplayName("should send a task larger than the request budget alone")
  void should_send_oversized_task_alone() {
    // Given: a task with about 5 MB of input ids, between two small tasks
    var manyIds = IntStream.range(0, 130_000).mapToObj(i -> BlobId.from(UUID.randomUUID().toString())).toList();
    var oversized = new TaskSubmission(manyIds, List.of(BlobId.from("out")), BlobId.from("payload-big"), null);

    // When
    taskService.submitTasks(List.of(submission(0), oversized, submission(1)));

    // Then: the Agent reports the error for the oversized task only
    assertThat(agentGrpcMock.submitTasksCalls).isEqualTo(3);
  }

  @Test
  @DisplayName("should reject the whole list without calling the Agent when a submission has no output")
  void should_reject_list_with_invalid_submission() {
    // Given
    var invalid = new TaskSubmission(List.of(BlobId.from("in")), List.of(), BlobId.from("payload"), null);
    var submissions = List.of(submission(0), invalid);

    // When / Then
    assertThatThrownBy(() -> taskService.submitTasks(submissions)).isInstanceOf(IllegalArgumentException.class);
    assertThat(agentGrpcMock.submitTasksCalls).isZero();
  }

  private static TaskSubmission submission(int i) {
    return new TaskSubmission(List.of(BlobId.from("in" + i)), List.of(BlobId.from("out" + i)), BlobId.from("payload" + i), null);
  }
}
