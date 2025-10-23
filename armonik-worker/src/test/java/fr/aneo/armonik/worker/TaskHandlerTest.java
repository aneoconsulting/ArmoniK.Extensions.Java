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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataRequest;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataResponse;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskHandlerTest {

  @TempDir
  private Path tempDir;
  private AgentFutureStub agentStub;
  private String payloadContent;


  @BeforeEach
  void setUp() {
    agentStub = Mockito.mock(AgentFutureStub.class);
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
  @DisplayName("Should not create TaskHandler when payload does not exist")
  void should_not_create_TaskHandler_when_Payload_does_not_exist() {
    // Given
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .build();

    // When - Then
    assertThatThrownBy(() -> TaskHandler.from(agentStub, request)).isInstanceOf(ArmoniKException.class);
  }

  @Test
  @DisplayName("Should not create TaskHandler when payload does not follow the convention")
  void should_not_create_TaskHandler_when_Payload_does_not_follow_convention() throws IOException {
    // Given
    writeString("payload-id", "payload not following convention");
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .build();

    // When - Then
    assertThatThrownBy(() -> TaskHandler.from(agentStub, request)).isInstanceOf(ArmoniKException.class);
  }

  @Test
  @DisplayName("Should not create TaskHandler when input file is missing")
  void should_not_create_TaskHandler_when_input_file_is_missing() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeRandomBytes("name-id");
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .build();
    // When - Then
    assertThatThrownBy(() -> TaskHandler.from(agentStub, request)).isInstanceOf(ArmoniKException.class);
  }

  @Test
  @DisplayName("Should create TaskHandler when payload follows the convention")
  void should_create_TaskHandler_when_payload_follows_convention() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeRandomBytes("name-id");
    writeRandomBytes("age-id");
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .build();
    // When
    var taskHandler = TaskHandler.from(agentStub, request);

    // Then
    assertThat(taskHandler).isNotNull();
  }

  @Test
  @DisplayName("Should throw an exception when input name is missing")
  void should_throw_exception_when_input_name_is_missing() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeString("name-id", "John Doe");
    writeString("age-id", "42");
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .build();
    var taskHandler = TaskHandler.from(agentStub, request);

    // When - Then
    assertThatThrownBy(() -> taskHandler.getInput("address")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("Should build inputs from payload association table")
  void should_build_inputs_from_payload_association_table() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeString("name-id", "John Doe");
    writeString("age-id", "42");
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .build();
    // When
    var taskHandler = TaskHandler.from(agentStub, request);

    // Then
    assertThat(taskHandler.inputs()).containsOnlyKeys("name", "age");
    assertThat(taskHandler.hasInput("name")).isTrue();
    assertThat(taskHandler.getInput("name").asString(UTF_8)).isEqualTo("John Doe");
    assertThat(taskHandler.hasInput("age")).isTrue();
    assertThat(taskHandler.getInput("age").asString(UTF_8)).isEqualTo("42");
  }

  @Test
  @DisplayName("Should build outputs from payload association table")
  void should_build_outputs_from_payload_association_table() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeString("name-id", "John Doe");
    writeString("age-id", "42");
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .build();

    // When
    var taskHandler = TaskHandler.from(agentStub, request);

    // Then
    assertThat(taskHandler.outputs()).containsOnlyKeys("result1", "result2");
    assertThat(taskHandler.hasOutput("result1")).isTrue();
    assertThat(taskHandler.getOutput("result1")).isNotNull();
    assertThat(taskHandler.getOutput("result2")).isNotNull();
    assertThat(taskHandler.hasOutput("result2")).isTrue();
  }

  @Test
  @DisplayName("Should throw an exception when output name is missing")
  void should_throw_exception_when_output_name_is_missing() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeString("name-id", "John Doe");
    writeString("age-id", "42");
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .build();
    var taskHandler = TaskHandler.from(agentStub, request);

    // When - Then
    assertThatThrownBy(() -> taskHandler.getOutput("address")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("Should notify Agent when output is written")
  void should_notify_agent_when_output_is_written() throws IOException {
    // Given
    writeString("payload-id", payloadContent);
    writeString("name-id", "John Doe");
    writeString("age-id", "42");
    var request = ProcessRequest.newBuilder()
                                .setDataFolder(tempDir.toString())
                                .setPayloadId("payload-id")
                                .setCommunicationToken("communication-token")
                                .setSessionId("session-id")
                                .build();
    var taskHandler = TaskHandler.from(agentStub, request);
    when(agentStub.notifyResultData(any())).thenReturn(immediateFuture(NotifyResultDataResponse.newBuilder().build()));

    // When
    taskHandler.getOutput("result1").write("Hello John", UTF_8);

    // Then
    var notifyRequestCaptor = ArgumentCaptor.forClass(NotifyResultDataRequest.class);
    verify(agentStub).notifyResultData(notifyRequestCaptor.capture());
    var notifyRequest = notifyRequestCaptor.getValue();
    assertThat(notifyRequest.getCommunicationToken()).isEqualTo("communication-token");
    assertThat(notifyRequest.getIdsCount()).isEqualTo(1);
    assertThat(notifyRequest.getIds(0).getResultId()).isEqualTo("result1-id");
    assertThat(notifyRequest.getIds(0).getSessionId()).isEqualTo("session-id");
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
