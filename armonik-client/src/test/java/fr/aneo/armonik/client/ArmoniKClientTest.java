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

import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.exception.ArmoniKException;
import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.SessionsGrpcMock;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArmoniKClientTest extends InProcessGrpcTestBase {

  private final SessionsGrpcMock sessionsGrpcMock = new SessionsGrpcMock();
  private ArmoniKClient client;
  private SessionDefinition sessionDefinition;
  private TaskConfiguration taskConfiguration;

  @BeforeEach
  void setUp() {
    taskConfiguration = new TaskConfiguration(3, 1, "partition_1", Duration.ofMinutes(60), Map.of("option1", "value1"));
    sessionDefinition = new SessionDefinition(Set.of("partition_1", "partition_2"), taskConfiguration);
    client = new ArmoniKClient(channelPool);
  }

  @Test
  @DisplayName("should open a new session successfully")
  void should_open_a_new_session_successfully() {
    // When
    var sessionHandle = client.openSession(sessionDefinition);

    // Then
    assertThat(sessionHandle.sessionInfo().id()).isNotNull();
    assertThat(sessionHandle.sessionInfo().partitionIds()).containsExactlyInAnyOrder("partition_1", "partition_2");
    assertThat(sessionHandle.sessionInfo().taskConfiguration()).isEqualTo(taskConfiguration);

    assertThat(sessionsGrpcMock.submittedCreateSessionRequest.getPartitionIdsList()).containsExactlyInAnyOrder("partition_1", "partition_2");
    assertThat(sessionsGrpcMock.submittedCreateSessionRequest.getDefaultTaskOption().getMaxRetries()).isEqualTo(3);
    assertThat(sessionsGrpcMock.submittedCreateSessionRequest.getDefaultTaskOption().getPartitionId()).isEqualTo("partition_1");
    assertThat(sessionsGrpcMock.submittedCreateSessionRequest.getDefaultTaskOption().getMaxDuration().getSeconds()).isEqualTo(3_600);
    assertThat(sessionsGrpcMock.submittedCreateSessionRequest.getDefaultTaskOption().getMaxDuration().getNanos()).isEqualTo(0);
    assertThat(sessionsGrpcMock.submittedCreateSessionRequest.getDefaultTaskOption().getPriority()).isEqualTo(1);
    assertThat(sessionsGrpcMock.submittedCreateSessionRequest.getDefaultTaskOption().getOptionsMap()).isEqualTo(Map.of("option1", "value1"));
  }

  @Test
  @DisplayName("should get an existing session")
  void should_get_an_existing_session() {
    // Given
    var sessionId = SessionId.from("session_1");

    // When
    var sessionHandle = client.getSession(sessionId);

    // Then
    assertThat(sessionsGrpcMock.submittedGetSessionRequest.getSessionId()).isEqualTo(sessionId.asString());
    assertThat(sessionHandle.sessionInfo()).isEqualTo(
      new SessionInfo(
        sessionId,
        Set.of("partition1", "partition2"),
        new TaskConfiguration(2, 5, "partition1", Duration.ofMinutes(60), Map.of("option1", "value1"))
      ));
  }

  @Test
  @DisplayName("should close a session")
  void should_close_an_existing_session() {
    // Given
    var sessionId = SessionId.from("session_1");

    // When
    client.closeSession(sessionId);

    // Then
    assertThat(sessionsGrpcMock.submittedCloseSessionRequest).isNotNull();
    assertThat(sessionsGrpcMock.submittedCloseSessionRequest.getSessionId()).isEqualTo(sessionId.asString());
  }

  @Test
  @DisplayName("should cancel a session")
  void should_cancel_an_existing_session() {
    // Given
    var sessionId = SessionId.from("session_1");

    // When
    client.cancelSession(sessionId);

    // Then
    assertThat(sessionsGrpcMock.submittedCancelSessionRequest).isNotNull();
    assertThat(sessionsGrpcMock.submittedCancelSessionRequest.getSessionId()).isEqualTo(sessionId.asString());
  }

  @Test
  @DisplayName("should throw exception when session does not exist")
  void should_throw_exception_when_session_does_not_exist() {
    // Given
    var sessionId = SessionId.from("does not exist");

    // When / Then
    assertThatThrownBy(() -> client.getSession(sessionId)).isInstanceOf(ArmoniKException.class);
  }

  @Test
  @DisplayName("should close channel pool when client is closed")
  void should_close_channel_pool_when_client_is_closed() {
    // When
    client.close();

    // Then
    assertThatThrownBy(() -> client.openSession(sessionDefinition)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("should fail to create session after client is closed")
  void should_fail_to_create_session_after_client_is_closed() {
    // Given
    client.close();

    // When/Then
    assertThatThrownBy(() -> client.openSession(sessionDefinition)).isInstanceOf(IllegalStateException.class);
  }

  @SuppressWarnings("resource")
  @Test
  @DisplayName("should throw NPE when config is null")
  void should_throw_npe_when_config_is_null() {
    assertThatThrownBy(() -> new ArmoniKClient((ArmoniKConfig) null)).isInstanceOf(NullPointerException.class);
  }

  @Override
  protected List<BindableService> services() {
    return List.of(sessionsGrpcMock);
  }
}
