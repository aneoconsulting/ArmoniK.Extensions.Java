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

import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import fr.aneo.armonik.worker.testutils.AgentGrpcMock;
import fr.aneo.armonik.worker.testutils.InProcessGrpcTestBase;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TaskServiceTest extends InProcessGrpcTestBase {

  private final AgentGrpcMock agentGrpcMock = new AgentGrpcMock();
  private TaskService taskService;

  @BeforeEach
  void setUp() {
    var agentStub = AgentGrpc.newFutureStub(channel);
    taskService = new TaskService(agentStub, SessionId.from("test-session"), "test-communication-token");
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
}
