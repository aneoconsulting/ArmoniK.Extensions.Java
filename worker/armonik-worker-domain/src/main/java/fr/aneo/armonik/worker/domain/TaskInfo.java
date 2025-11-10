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
package fr.aneo.armonik.worker.domain;

import java.util.Objects;

/**
 * Immutable metadata describing a submitted task in the ArmoniK cluster.
 * <p>
 * This class represents task information as provided by the ArmoniK cluster
 * after successful task submission, including the task's unique identifier
 * assigned by the cluster. Instances are typically created by the worker
 * infrastructure when tasks are submitted to the cluster.
 * </p>
 *
 * <h2>Usage Context</h2>
 * <p>
 * <strong>Production:</strong> Task information is created by the worker infrastructure
 * after successfully submitting a task to the cluster (via gRPC). Applications receive
 * {@link TaskInfo} instances through {@link TaskHandle#deferredTaskInfo()}.
 * </p>
 *
 * <h2>Equality</h2>
 * <p>
 * Two {@code TaskInfo} instances are considered equal if they have the same task ID.
 * This reflects the fact that a task's identity is determined solely by its unique
 * cluster-assigned identifier.
 * </p>
 *
 * @see TaskHandle
 * @see TaskId
 */
public final class TaskInfo {
  private final TaskId id;

  /**
   * Creates task information with the specified task identifier.
   * <p>
   * <strong>Usage Context:</strong> In production environments, task information is
   * typically created by the worker infrastructure after successful task submission.
   * This public constructor is primarily provided for testing purposes when implementing
   * custom task processors.
   * </p>
   *
   * @param id the unique task identifier assigned by the cluster; must not be {@code null}
   * @throws NullPointerException if {@code id} is {@code null}
   */
  public TaskInfo(TaskId id) {
    this.id = Objects.requireNonNull(id, "id cannot be null");
  }

  /**
   * Returns the unique identifier of this task.
   *
   * @return the task identifier; never {@code null}
   */
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
    return "TaskInfo[id=" + id + ']';
  }
}
