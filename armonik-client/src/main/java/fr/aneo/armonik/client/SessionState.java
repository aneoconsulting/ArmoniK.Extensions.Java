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

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Immutable snapshot of a session's state in the ArmoniK cluster.
 * <p>
 * A {@code SessionState} instance mirrors the information returned by the Sessions service
 * for a given session. It aggregates the current lifecycle status, submission flags,
 * configuration and key timestamps into a single value object. Instances of this record
 * are typically obtained from session lifecycle operations such as
 * {@link SessionHandle#cancel()}, {@link SessionHandle#pause()}, {@link SessionHandle#resume()},
 * {@link SessionHandle#close()}, {@link SessionHandle#purge()} and {@link SessionHandle#delete()},
 * or from {@link ArmoniKClient#closeSession(SessionId)}.
 * <p>
 * Time-related fields may be {@code null} when the corresponding lifecycle transition
 * has not occurred yet. For example, {@code cancelledAt} is only populated when the
 * session has been cancelled, and {@code closedAt} is only populated after the session
 * has been closed. The {@code duration} field is generally set when the session is in a
 * terminal state (such as {@code CANCELLED} or {@code CLOSED}) and represents the elapsed
 * time between creation and termination as computed by the control plane.
 *
 * @param sessionId         the unique identifier of the session
 * @param status            the current lifecycle status of the session as reported by the cluster
 * @param clientSubmission  whether clients are currently allowed to submit new tasks in this session
 * @param workerSubmission  whether workers are currently allowed to submit tasks in this session
 * @param partitionIds      the set of partition identifiers associated with this session; determines where
 *                          tasks may be scheduled
 * @param taskConfiguration the default task configuration applied to tasks created in this session
 * @param createdAt         the instant at which the session was created in the cluster; never {@code null}
 *                          for a valid session
 * @param cancelledAt       the instant at which the session was cancelled, or {@code null} if the session
 *                          has not been cancelled
 * @param closedAt          the instant at which the session was closed, or {@code null} if the session
 *                          has not been closed
 * @param purgedAt          the instant at which the session's data was purged from storage, or {@code null}
 *                          if no purge has been performed
 * @param deletedAt         the instant at which the session was deleted from the control plane, or {@code null}
 *                          if the session still exists
 * @param duration          the total duration of the session as computed by the control plane, typically set
 *                          when the session reaches a terminal state; may be {@code null} otherwise
 * @see SessionHandle
 * @see SessionStatus
 * @see TaskConfiguration
 */
public record SessionState(
  SessionId sessionId,
  SessionStatus status,
  boolean clientSubmission,
  boolean workerSubmission,
  List<String> partitionIds,
  TaskConfiguration taskConfiguration,
  Instant createdAt,
  Instant cancelledAt,
  Instant closedAt,
  Instant purgedAt,
  Instant deletedAt,
  Duration duration
) {
}
