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
package fr.aneo.armonik.client.internal.grpc.mappers;

import com.google.protobuf.Timestamp;
import fr.aneo.armonik.api.grpc.v1.sessions.SessionStatusOuterClass;
import fr.aneo.armonik.client.SessionState;
import fr.aneo.armonik.client.SessionStatus;
import fr.aneo.armonik.client.TaskConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static fr.aneo.armonik.api.grpc.v1.Objects.TaskOptions;
import static fr.aneo.armonik.api.grpc.v1.sessions.SessionsCommon.SessionRaw;
import static org.assertj.core.api.Assertions.assertThat;

class SessionMapperTest {

  @Test
  void should_map_session_raw_to_session_state() {
    // Given
    var createdAt = Timestamp.newBuilder().setSeconds(100).setNanos(200).build();
    var cancelledAt = Timestamp.newBuilder().setSeconds(300).setNanos(400).build();
    var closedAt = Timestamp.newBuilder().setSeconds(500).setNanos(600).build();
    var purgedAt = Timestamp.newBuilder().setSeconds(700).setNanos(800).build();
    var deletedAt = Timestamp.newBuilder().setSeconds(900).setNanos(1000).build();
    var duration = com.google.protobuf.Duration.newBuilder()
                                               .setSeconds(42)
                                               .setNanos(123_000_000)
                                               .build();

    var taskOptions = TaskOptions.newBuilder()
                                 .setMaxRetries(3)
                                 .setPriority(5)
                                 .setPartitionId("partition-1")
                                 .setMaxDuration(com.google.protobuf.Duration.newBuilder()
                                                                             .setSeconds(600)
                                                                             .setNanos(0))
                                 .putAllOptions(Map.of("opt1", "val1"))
                                 .build();

    var raw = SessionRaw.newBuilder()
                        .setSessionId("session-123")
                        .setStatus(SessionStatusOuterClass.SessionStatus.SESSION_STATUS_CANCELLED)
                        .setClientSubmission(true)
                        .setWorkerSubmission(false)
                        .addAllPartitionIds(List.of("p1", "p2"))
                        .setOptions(taskOptions)
                        .setCreatedAt(createdAt)
                        .setCancelledAt(cancelledAt)
                        .setClosedAt(closedAt)
                        .setPurgedAt(purgedAt)
                        .setDeletedAt(deletedAt)
                        .setDuration(duration)
                        .build();

    // When
    SessionState sessionState = SessionMapper.toSessionState(raw);

    // Then
    assertThat(sessionState.sessionId().asString()).isEqualTo("session-123");
    assertThat(sessionState.status()).isEqualTo(SessionStatus.CANCELLED);
    assertThat(sessionState.clientSubmission()).isTrue();
    assertThat(sessionState.workerSubmission()).isFalse();
    assertThat(sessionState.partitionIds()).containsExactly("p1", "p2");
    assertThat(sessionState.taskConfiguration())
      .usingRecursiveComparison()
      .isEqualTo(new TaskConfiguration(
        3,
        5,
        "partition-1",
        Duration.ofSeconds(600),
        Map.of("opt1", "val1")
      ));

    assertThat(sessionState.createdAt()).isEqualTo(Instant.ofEpochSecond(100, 200));
    assertThat(sessionState.cancelledAt()).isEqualTo(Instant.ofEpochSecond(300, 400));
    assertThat(sessionState.closedAt()).isEqualTo(Instant.ofEpochSecond(500, 600));
    assertThat(sessionState.purgedAt()).isEqualTo(Instant.ofEpochSecond(700, 800));
    assertThat(sessionState.deletedAt()).isEqualTo(Instant.ofEpochSecond(900, 1000));
    assertThat(sessionState.duration()).isEqualTo(Duration.ofSeconds(42, 123_000_000));
  }

  @Test
  void should_set_time_fields_to_null_when_missing() {
    // Given
    var taskOptions = TaskOptions.newBuilder()
                                 .setMaxRetries(3)
                                 .setPriority(5)
                                 .setPartitionId("partition-1")
                                 .setMaxDuration(com.google.protobuf.Duration.newBuilder()
                                                                             .setSeconds(600)
                                                                             .setNanos(0));
    var raw = SessionRaw.newBuilder()
                        .setSessionId("session-123")
                        .setOptions(taskOptions)
                        .setStatus(SessionStatusOuterClass.SessionStatus.SESSION_STATUS_RUNNING)
                        .build();

    // When
    SessionState state = SessionMapper.toSessionState(raw);

    // Then
    assertThat(state.status()).isEqualTo(SessionStatus.RUNNING);
    assertThat(state.createdAt()).isNull();
    assertThat(state.cancelledAt()).isNull();
    assertThat(state.closedAt()).isNull();
    assertThat(state.purgedAt()).isNull();
    assertThat(state.deletedAt()).isNull();
    assertThat(state.duration()).isNull();
  }
}
