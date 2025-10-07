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
package fr.aneo.armonik.client.model;

import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.SessionsGrpcMock;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ArmoniKClientTest extends InProcessGrpcTestBase {

  private final SessionsGrpcMock sessionsGrpcMock = new SessionsGrpcMock();
  private ArmoniKClient client;

  @BeforeEach
  void setUp() {
    client = new ArmoniKClient(channel);
  }

  @Test
  void should_open_a_new_session_successfully() {
    // Given
    var taskConfiguration = new TaskConfiguration(3, 1, "partition_1", Duration.ofMinutes(60), Map.of("option1", "value1"));
    var sessionDefinition = new SessionDefinition(Set.of("partition_1", "partition_2"), taskConfiguration);

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

  @Override
  protected List<BindableService> services() {
    return List.of(sessionsGrpcMock);
  }
}
