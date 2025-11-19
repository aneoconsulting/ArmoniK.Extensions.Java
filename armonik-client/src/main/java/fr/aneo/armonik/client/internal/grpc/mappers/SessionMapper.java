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
import fr.aneo.armonik.client.SessionId;
import fr.aneo.armonik.client.SessionState;
import fr.aneo.armonik.client.SessionStatus;
import fr.aneo.armonik.client.definition.SessionDefinition;

import java.time.Duration;
import java.time.Instant;

import static fr.aneo.armonik.api.grpc.v1.sessions.SessionsCommon.*;
import static fr.aneo.armonik.client.internal.grpc.mappers.TaskMapper.toTaskConfiguration;
import static fr.aneo.armonik.client.internal.grpc.mappers.TaskMapper.toTaskOptions;

public final class SessionMapper {

  private SessionMapper() {
  }

  public static CreateSessionRequest toCreateSessionRequest(SessionDefinition sessionDefinition) {
    return CreateSessionRequest.newBuilder()
                               .addAllPartitionIds(sessionDefinition.partitionIds())
                               .setDefaultTaskOption(toTaskOptions(sessionDefinition.taskConfiguration()))
                               .build();
  }

  public static GetSessionRequest toGetSessionRequest(SessionId sessionId) {
    return GetSessionRequest.newBuilder()
                            .setSessionId(sessionId.asString())
                            .build();
  }

  public static CancelSessionRequest toCancelSessionRequest(SessionId sessionId) {
    return CancelSessionRequest.newBuilder().setSessionId(sessionId.asString()).build();
  }

  public static PauseSessionRequest toPauseSessionRequest(SessionId sessionId) {
    return PauseSessionRequest.newBuilder().setSessionId(sessionId.asString()).build();
  }

  public static ResumeSessionRequest toResumeSessionRequest(SessionId sessionId) {
    return ResumeSessionRequest.newBuilder().setSessionId(sessionId.asString()).build();
  }

  public static CloseSessionRequest toCloseSessionRequest(SessionId sessionId) {
    return CloseSessionRequest.newBuilder().setSessionId(sessionId.asString()).build();
  }

  public static PurgeSessionRequest toPurgeSessionRequest(SessionId sessionId) {
    return PurgeSessionRequest.newBuilder().setSessionId(sessionId.asString()).build();
  }

  public static DeleteSessionRequest toDeleteSessionRequest(SessionId sessionId) {
    return DeleteSessionRequest.newBuilder().setSessionId(sessionId.asString()).build();
  }

  public static SessionState toSessionState(SessionRaw sessionRaw) {
    return new SessionState(
      SessionId.from(sessionRaw.getSessionId()),
      toSessionStatus(sessionRaw.getStatus()),
      sessionRaw.getClientSubmission(),
      sessionRaw.getWorkerSubmission(),
      sessionRaw.getPartitionIdsList(),
      toTaskConfiguration(sessionRaw.getOptions()),
      sessionRaw.hasCreatedAt() ? toInstant(sessionRaw.getCreatedAt()) : null,
      sessionRaw.hasCancelledAt() ? toInstant(sessionRaw.getCancelledAt()) : null,
      sessionRaw.hasClosedAt() ? toInstant(sessionRaw.getClosedAt()) : null,
      sessionRaw.hasPurgedAt() ? toInstant(sessionRaw.getPurgedAt()) : null,
      sessionRaw.hasDeletedAt() ? toInstant(sessionRaw.getDeletedAt()) : null,
      sessionRaw.hasDuration() ? toDuration(sessionRaw.getDuration()) : null
    );
  }

  private static SessionStatus toSessionStatus(SessionStatusOuterClass.SessionStatus sessionStatus) {
    return switch (sessionStatus) {
      case SESSION_STATUS_UNSPECIFIED -> SessionStatus.UNSPECIFIED;
      case SESSION_STATUS_RUNNING -> SessionStatus.RUNNING;
      case SESSION_STATUS_PAUSED -> SessionStatus.PAUSED;
      case SESSION_STATUS_CANCELLED -> SessionStatus.CANCELLED;
      case SESSION_STATUS_CLOSED -> SessionStatus.CLOSED;
      case SESSION_STATUS_PURGED -> SessionStatus.PURGED;
      case SESSION_STATUS_DELETED -> SessionStatus.DELETED;
      default -> throw new IllegalArgumentException("Unknown session status: " + sessionStatus);
    };
  }

  private static Duration toDuration(com.google.protobuf.Duration protobufDuration) {
    return Duration.ofSeconds(protobufDuration.getSeconds(), protobufDuration.getNanos());
  }

  private static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
