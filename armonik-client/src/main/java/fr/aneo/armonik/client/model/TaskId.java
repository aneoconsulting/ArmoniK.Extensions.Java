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

import java.util.Objects;

/**
 * Immutable identifier for a task within the ArmoniK distributed computing platform.
 * <p>
 * Task identifiers are assigned by the ArmoniK cluster during task submission and
 * uniquely identify tasks across the entire cluster. This class provides a type-safe
 * wrapper around the string-based task identifier used by the underlying gRPC API.
 * <p>
 * TaskId instances are immutable and implement proper equality and hash code semantics
 * for use in collections and as map keys.
 *
 * @see TaskInfo
 * @see TaskHandle
 */
public final class TaskId {
  private final String id;

  private TaskId(String id) {
    this.id = id;
  }

  /**
   * Returns the string representation of this task identifier.
   *
   * @return the task identifier as a string
   */
  public String asString() {
    return id;
  }

  /**
   * Creates a task identifier from the given string.
   * <p>
   * This factory method creates blob identifiers from cluster-assigned string
   * identifiers received through the gRPC API.
   *
   * @param id the string identifier assigned by the ArmoniK cluster
   * @return a new TaskId instance wrapping the given identifier
   * @throws NullPointerException if id is null
   */
   static TaskId from(String id) {
    return new TaskId(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (TaskId) obj;
    return Objects.equals(this.id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return "TaskId{" +
      "id='" + id + '\'' +
      '}';
  }
}
