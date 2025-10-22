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
import fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class DefaultTaskContextFactoryTest {

  @TempDir
  private Path tempDir;
  private AgentGrpc.AgentFutureStub agentStub;
  private String payloadContent;
  private DefaultTaskContextFactory contextFactory;


  @BeforeEach
  void setUp() {
    agentStub = Mockito.mock(AgentGrpc.AgentFutureStub.class);
    contextFactory = new DefaultTaskContextFactory();

    when(agentStub.withDeadlineAfter(anyLong(), eq(MILLISECONDS))).thenReturn(agentStub);
    payloadContent = """
      {
        "inputs": {
          "name": "name-id",
          "age": "age-id"
        },
        "outputs": {
          "result1": "result1-id",
          "result2": "result2-id"
        }
      }
      """;
  }

  @Test
  @DisplayName("Should not create task context when payload does not exist")
  void should_not_create_task_context_when_Payload_does_not_exist() {
    // Given
    var request = WorkerCommon.ProcessRequest.newBuilder()
                                             .setDataFolder(tempDir.toString())
                                             .setPayloadId("payload-id")
                                             .build();

    // When - Then
    assertThatThrownBy(() -> contextFactory.create(agentStub, request)).isInstanceOf(ArmoniKException.class);
  }

  @Test
  @DisplayName("Should not create task context when payload does not follow the convention")
  void should_not_create_task_context_when_Payload_does_not_follow_convention() throws IOException {
    // Given
    writeString("payload-id", "payload not following convention");
    var request = WorkerCommon.ProcessRequest.newBuilder()
                                             .setDataFolder(tempDir.toString())
                                             .setPayloadId("payload-id")
                                             .build();

    // When - Then
    assertThatThrownBy(() -> contextFactory.create(agentStub, request)).isInstanceOf(ArmoniKException.class);
  }

  @Test
  @DisplayName("Should not create task context when input file is missing")
  void should_not_create_task_context_when_input_file_is_missing() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeRandomBytes("name-id");
    var request = WorkerCommon.ProcessRequest.newBuilder()
                                             .setDataFolder(tempDir.toString())
                                             .setPayloadId("payload-id")
                                             .build();
    // When - Then
    assertThatThrownBy(() -> contextFactory.create(agentStub, request)).isInstanceOf(ArmoniKException.class);
  }

  @Test
  @DisplayName("Should create task context when payload follows the convention")
  void should_create_task_context_when_payload_follows_convention() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeRandomBytes("name-id");
    writeRandomBytes("age-id");
    var request = WorkerCommon.ProcessRequest.newBuilder()
                                             .setDataFolder(tempDir.toString())
                                             .setPayloadId("payload-id")
                                             .build();
    // When
    var taskContext = contextFactory.create(agentStub, request);

    // Then
    assertThat(taskContext).isNotNull();
  }


  @Test
  @DisplayName("Should build inputs from payload association table")
  void should_build_inputs_from_payload_association_table() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeString("name-id", "John Doe");
    writeString("age-id", "42");
    var request = WorkerCommon.ProcessRequest.newBuilder()
                                             .setDataFolder(tempDir.toString())
                                             .setPayloadId("payload-id")
                                             .build();
    // When
    var taskContext = contextFactory.create(agentStub, request);

    // Then
    assertThat(taskContext.inputs()).containsOnlyKeys("name", "age");
    assertThat(taskContext.hasInput("name")).isTrue();
    assertThat(taskContext.getInput("name").asString(UTF_8)).isEqualTo("John Doe");
    assertThat(taskContext.hasInput("age")).isTrue();
    assertThat(taskContext.getInput("age").asString(UTF_8)).isEqualTo("42");
  }

  @Test
  @DisplayName("Should build outputs from payload association table")
  void should_build_outputs_from_payload_association_table() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeString("name-id", "John Doe");
    writeString("age-id", "42");
    var request = WorkerCommon.ProcessRequest.newBuilder()
                                             .setDataFolder(tempDir.toString())
                                             .setPayloadId("payload-id")
                                             .build();

    // When
    var taskContext = contextFactory.create(agentStub, request);

    // Then
    assertThat(taskContext.outputs()).containsOnlyKeys("result1", "result2");
    assertThat(taskContext.hasOutput("result1")).isTrue();
    assertThat(taskContext.getOutput("result1")).isNotNull();
    assertThat(taskContext.getOutput("result2")).isNotNull();
    assertThat(taskContext.hasOutput("result2")).isTrue();
  }


  private void writeString(String name, String content) throws IOException {
    var path = tempDir.resolve(name);
    Files.writeString(path, content, UTF_8);
  }

  private void writeRandomBytes(String name) throws IOException {
    var path = tempDir.resolve(name);
    byte[] data = new byte[10];
    new Random(1234).nextBytes(data);
    Files.write(path, data);
  }

}
