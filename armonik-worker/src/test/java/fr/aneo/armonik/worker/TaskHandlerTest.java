package fr.aneo.armonik.worker;

import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskHandlerTest {

  @TempDir
  private Path tempDir;
  private AgentGrpc.AgentFutureStub mock;
  private String payloadContent;


  @BeforeEach
  void setUp() {
    mock = Mockito.mock(AgentGrpc.AgentFutureStub.class);
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
    assertThatThrownBy(() -> TaskHandler.from(mock, request))
      .isInstanceOf(ArmoniKException.class);
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
    assertThatThrownBy(() -> TaskHandler.from(mock, request))
      .isInstanceOf(ArmoniKException.class);
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
    assertThatThrownBy(() -> TaskHandler.from(mock, request))
      .isInstanceOf(ArmoniKException.class);
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
    var taskHandler = TaskHandler.from(mock, request);

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
    var taskHandler = TaskHandler.from(mock, request);

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
    var taskHandler = TaskHandler.from(mock, request);

    // Then
    assertThat(taskHandler.hasInput("name")).isTrue();
    assertThat(taskHandler.getInput("name").asString(UTF_8)).isEqualTo("John Doe");
    assertThat(taskHandler.hasInput("age")).isTrue();
    assertThat(taskHandler.getInput("age").asString(UTF_8)).isEqualTo("42");
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
