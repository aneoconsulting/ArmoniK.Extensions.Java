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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Encapsulates the default execution options for a task submitted to ArmoniK.
 * <p>
 * These options are used by the scheduler to influence routing, retries, and execution constraints for a task.
 * </p>
 *
 * <ul>
 *   <li><b>maxRetries</b> — Maximum number of execution attempts performed by ArmoniK
 *       in case of task failure. A value of 1 means a single attempt with no retry.</li>
 *
 *   <li><b>priority</b> — Relative importance of the task. Higher values denote higher priority for scheduling.</li>
 *
 *   <li><b>partitionId</b> — Identifier of the partition to target when dispatching tasks.
 *       Partitions are logical lanes that route work to specific pools of compute resources.
 *       An empty string or a {@code null} value indicates that the default partition defined in ArmoniK
 *       should be used.</li>
 *
 *   <li><b>maxDuration</b> — Maximum wall-clock duration allowed for a task run. If exceeded,
 *       ArmoniK may consider the task as timed out.</li>
 *
 *   <li><b>options</b> — Additional free-form key/value options for application-specific needs
 *       or future extensions.</li>
 * </ul>
 *
 * @param maxRetries  maximum number of attempts for the task execution (>= 1)
 * @param priority    scheduling priority of the task (>= 1)
 * @param partitionId target partition identifier; empty or {@code null} delegates to ArmoniK's default partition
 * @param maxDuration maximum wall-clock duration for the task; defaults to 60 minutes when null
 * @param options     additional, implementation-specific options
 *
 */
public record TaskConfiguration(
  int maxRetries,
  int priority,
  String partitionId,
  Duration maxDuration,
  Map<String, String> options) {

  /**
   * Compact constructor applying minimal validation and defaulting rules:
   * <ul>
   *   <li>Validates that {@code maxRetries} and {@code priority} are at least 1.</li>
   *   <li>Normalizes {@code partitionId} to empty string if null (which means: use ArmoniK's default partition).</li>
   *   <li>Defaults {@code maxDuration} to 60 minutes if null.</li>
   *   <li>Defaults {@code options} to an empty mutable map if null.</li>
   * </ul>
   *
   * @throws IllegalArgumentException if {@code maxRetries} &lt; 1 or {@code priority} &lt; 1
   */
  public TaskConfiguration {
    if (maxRetries < 1) throw new IllegalArgumentException("maxRetries must be greater than 1");
    if (priority < 1) throw new IllegalArgumentException("priority must be greater than 1");

    partitionId = partitionId == null ? "" : partitionId;
    maxDuration = maxDuration == null ? Duration.ofMinutes(60) : maxDuration;
    options = options == null ? new HashMap<>() : options;
  }

  /**
   * Indicates whether this configuration explicitly targets a partition.
   * <p>
   * Returns {@code false} when the configuration delegates to ArmoniK's default partition
   * (i.e., when {@code partitionId} is {@code null} or empty).
   * </p>
   *
   * @return true if {@code partitionId} is non-null and not empty; false otherwise
   */
  public boolean hasExplicitPartition() {
    return !isNullOrEmpty(partitionId);
  }

  /**
   * Creates a default TaskConfiguration with:
   * <ul>
   *   <li>{@code maxRetries = 2}</li>
   *   <li>{@code priority = 1}</li>
   *   <li>no explicit {@code partitionId} (uses ArmoniK's default partition)</li>
   *   <li>{@code maxDuration = 60 minutes}</li>
   *   <li>empty {@code options}</li>
   * </ul>
   *
   * @return a default configuration suitable for most simple use cases
   */
  public static TaskConfiguration defaultConfiguration() {
    return TaskConfiguration.defaultConfigurationWithPartition(null);
  }

  /**
   * Creates a default TaskConfiguration that targets a specific partition.
   * <p>
   * If {@code partitionId} is {@code null} or empty, this configuration delegates to the default
   * partition defined in ArmoniK.
   * </p>
   *
   * @param partitionId the partition to target; {@code null} or empty means use ArmoniK's default partition
   * @return a default configuration bound to the given partition (if any)
   */
  public static TaskConfiguration defaultConfigurationWithPartition(String partitionId) {
    return new TaskConfiguration(2, 1, partitionId, null, new HashMap<>());
  }
}
