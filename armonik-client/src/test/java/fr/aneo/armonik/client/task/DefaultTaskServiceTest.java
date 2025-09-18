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
package fr.aneo.armonik.client.task;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.migrationsupport.rules.ExternalResourceSupport;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static fr.aneo.armonik.client.blob.BlobHandleFixture.blobHandle;
import static fr.aneo.armonik.client.session.SessionHandleFixture.sessionHandle;
import static org.assertj.core.api.Assertions.assertThat;

class DefaultTaskServiceTest {

  @RegisterExtension
  public static final ExternalResourceSupport externalResourceSupport = new ExternalResourceSupport();

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private DefaultTaskService taskService;
  private TasksServiceMock tasksServiceMock;

  @BeforeEach
  void setUp() throws IOException {
    tasksServiceMock = new TasksServiceMock();
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                                               .directExecutor()
                                               .addService(tasksServiceMock)
                                               .build()
                                               .start());

    var channel = grpcCleanup.register(InProcessChannelBuilder.forName(serverName)
                                                              .directExecutor()
                                                              .build());
    taskService = new DefaultTaskService(channel);
  }

  @Test
  void should_submit_task_successfully() {
    // Given
    var sessionHandle = sessionHandle();
    var payload = blobHandle(sessionHandle, UUID.fromString("55555555-5555-5555-5555-555555555555"));
    var inputs = Map.of(
      "name", blobHandle(sessionHandle, UUID.fromString("11111111-1111-1111-1111-111111111111")),
      "age", blobHandle(sessionHandle, UUID.fromString("22222222-2222-2222-2222-222222222222"))
    );
    var outputs = Map.of(
      "result1", blobHandle(sessionHandle, UUID.fromString("33333333-3333-3333-3333-333333333333")),
      "result2", blobHandle(sessionHandle, UUID.fromString("44444444-4444-4444-4444-444444444444"))
    );
    var taskConfiguration = new TaskConfiguration(3, 1, "partition_1", Duration.ofMinutes(60), Map.of("option1", "value1"));

    // When
    var taskHandle = taskService.submitTask(sessionHandle, inputs, outputs, payload, taskConfiguration);

    // then
    assertThat(taskHandle.sessionHandle()).isEqualTo(sessionHandle);
    assertThat(taskHandle.metadata().toCompletableFuture().join()).isNotNull();
    assertThat(taskHandle.inputs()).isEqualTo(inputs);
    assertThat(taskHandle.outputs()).isEqualTo(outputs);
    assertThat(taskHandle.taskConfiguration()).isEqualTo(taskConfiguration);
    assertThat(taskHandle.payLoad()).isEqualTo(payload);

    // payload
    assertThat(tasksServiceMock.submittedTasksRequest.getSessionId()).isEqualTo(sessionHandle.id().toString());

    // Task options
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskOptions().getMaxRetries()).isEqualTo(3);
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskOptions().getPartitionId()).isEqualTo("partition_1");
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskOptions().getMaxDuration().getSeconds()).isEqualTo(3_600);
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskOptions().getMaxDuration().getNanos()).isEqualTo(0);
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskOptions().getPriority()).isEqualTo(1);
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskOptions().getOptionsMap()).isEqualTo(Map.of("option1", "value1"));

    // Task Creation
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskCreationsCount()).isEqualTo(1);
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskCreations(0).getPayloadId()).isEqualTo("55555555-5555-5555-5555-555555555555");
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskCreations(0).getDataDependenciesList())
      .containsExactlyInAnyOrder("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222");
    assertThat(tasksServiceMock.submittedTasksRequest.getTaskCreations(0).getExpectedOutputKeysList())
      .containsExactlyInAnyOrder("33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444");
  }
}
