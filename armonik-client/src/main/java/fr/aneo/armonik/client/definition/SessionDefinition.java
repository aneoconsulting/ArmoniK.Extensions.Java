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
package fr.aneo.armonik.client.definition;

import fr.aneo.armonik.client.model.ArmoniKClient;
import fr.aneo.armonik.client.model.BlobCompletionListener;
import fr.aneo.armonik.client.model.SessionHandle;
import fr.aneo.armonik.client.model.TaskConfiguration;

import java.util.Set;


/**
 * Definition specifying the configuration and parameters for creating a session in ArmoniK.
 * <p>
 * A session definition contains the essential configuration needed to create a session
 * within the ArmoniK distributed computing platform. It specifies the partitions where
 * tasks can be executed, default task configuration, and the listener for handling
 * task output completion events.
 * <p>
 * Partitions in ArmoniK represent logical groupings that route work to specific pools
 * of compute resources [[1]](https://armonik.readthedocs.io/en/latest/content/user-guide/how-to-configure-partitions.html).
 * Sessions can target multiple partitions to distribute tasks across different resource pools.
 *
 * @param partitionIds the set of partition identifiers where tasks within this session can be executed
 * @param taskConfiguration the default task configuration applied to tasks submitted within this session
 * @param taskOutputsListener the listener to receive task output completion events for this session
 * @see ArmoniKClient#openSession(SessionDefinition)
 * @see SessionHandle
 * @see TaskConfiguration
 * @see BlobCompletionListener
 */
public record SessionDefinition(
  Set<String> partitionIds,
  TaskConfiguration taskConfiguration,
  BlobCompletionListener taskOutputsListener
) {

  /**
   * Creates a session definition targeting the specified partitions with the default task configuration.
   * <p>
   * This convenience constructor creates a session definition with the default task configuration
   * and no task output listener. Tasks submitted within this session will use the ArmoniK
   * {@link TaskConfiguration#defaultConfiguration()}
   *
   * @param partitionIds the set of partition identifiers where tasks can be executed
   * @throws NullPointerException if partitionIds is null
   * @see TaskConfiguration#defaultConfiguration()
   */
  public SessionDefinition(Set<String> partitionIds) {
    this(partitionIds, TaskConfiguration.defaultConfiguration(), null);
  }

  /**
   * Creates a session definition with the specified partitions and task configuration.
   * <p>
   * This convenience constructor creates a session definition with the provided
   * task configuration as the default for all tasks submitted within the session,
   * but without a task output listener.
   *
   * @param partitionIds the set of partition identifiers where tasks can be executed
   * @param taskConfiguration the default task configuration for tasks in this session
   * @throws NullPointerException if any parameter is null
   * @see TaskConfiguration
   */
  public SessionDefinition(Set<String> partitionIds, TaskConfiguration taskConfiguration) {
    this(partitionIds, taskConfiguration, null);
  }

  /**
   * Compact constructor that applies validation to the session definition parameters.
   * <p>
   * This constructor validates that:
   * <ul>
   *   <li>Partition IDs are not null or empty</li>
   *   <li>Task configuration is consistent with the specified partitions</li>
   *   <li>All parameters meet the requirements for session creation</li>
   * </ul>
   *
   * @throws NullPointerException if partitionIds is null
   * @throws IllegalArgumentException if partitionIds is empty or if task configuration
   *         specifies a partition not included in the session's partition set
   */
  public SessionDefinition {
    taskConfiguration = taskConfiguration == null ? TaskConfiguration.defaultConfiguration() : taskConfiguration;
    partitionIds = partitionIds == null ? Set.of() : partitionIds;
    validateTaskDefaultPartition(partitionIds, taskConfiguration);
  }

  private void validateTaskDefaultPartition(Set<String> sessionPartitions, TaskConfiguration taskConfiguration) {
    if (!sessionPartitions.isEmpty() &&
      taskConfiguration.hasExplicitPartition() &&
      !sessionPartitions.contains(taskConfiguration.partitionId())) {
      throw new IllegalArgumentException(
        "TaskConfiguration.partitionId (" + taskConfiguration.partitionId() + ") must be one from client's partitionIds " + sessionPartitions);
    }
  }
}
