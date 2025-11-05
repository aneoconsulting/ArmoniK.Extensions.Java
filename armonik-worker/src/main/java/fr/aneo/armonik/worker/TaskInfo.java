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

import java.util.Objects;

/**
 * Immutable metadata describing a submitted task in the ArmoniK cluster.
 * <p>
 * This record encapsulates the essential information about a task after it has been
 * acknowledged and assigned identifiers by the ArmoniK cluster. The task information
 * becomes available through {@link TaskHandle#deferredTaskInfo()} once the cluster
 * processes the task submission.
 *
 * @see TaskHandle#deferredTaskInfo()
 * @see TaskId
 */
public final class TaskInfo {
  private final TaskId id;

  /**
   * @param id the unique identifier assigned to the task by the ArmoniK cluster
   *
   */
  TaskInfo(TaskId id) {
    this.id = id;
  }

  public TaskId id() {
    return id;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (TaskInfo) obj;
    return Objects.equals(this.id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return "TaskInfo[" +
      "id=" + id + ']';
  }
}
