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

import java.util.Objects;
import java.util.Set;

/**
 * Immutable metadata describing a session within the ArmoniK distributed computing platform.
 * <p>
 * This record encapsulates the essential information about a session after it has been
 * created and acknowledged by the ArmoniK cluster. The session information becomes
 * available through {@link SessionHandle#sessionInfo()} and provides access to
 * cluster-assigned identifiers and configuration details.
 *
 * @see SessionHandle#sessionInfo()
 * @see SessionId
 * @see TaskConfiguration
 */
public final class SessionInfo {
  private final SessionId id;
  private final Set<String> partitionIds;
  private final TaskConfiguration taskConfiguration;

  /**
   * @param id                the unique identifier assigned to the session by the ArmoniK cluster
   * @param partitionIds      the set of partition identifiers where tasks within this session can be executed
   * @param taskConfiguration the default task configuration applied to tasks submitted within this session
   *
   */
  SessionInfo(SessionId id, Set<String> partitionIds, TaskConfiguration taskConfiguration) {
    this.id = id;
    this.partitionIds = partitionIds;
    this.taskConfiguration = taskConfiguration;
  }

  public SessionId id() {
    return id;
  }

  public Set<String> partitionIds() {
    return partitionIds;
  }

  public TaskConfiguration taskConfiguration() {
    return taskConfiguration;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (SessionInfo) obj;
    return Objects.equals(this.id, that.id) &&
      Objects.equals(this.partitionIds, that.partitionIds) &&
      Objects.equals(this.taskConfiguration, that.taskConfiguration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, partitionIds, taskConfiguration);
  }

  @Override
  public String toString() {
    return "SessionInfo[" +
      "id=" + id + ", " +
      "partitionIds=" + partitionIds + ", " +
      "taskConfiguration=" + taskConfiguration + ']';
  }

}
