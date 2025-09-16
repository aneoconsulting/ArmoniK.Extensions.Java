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

import fr.aneo.armonik.client.task.TaskConfiguration;

import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Fluent builder for {@link ArmoniKClient}.
 * <p>
 * Configure connection parameters, the default {@link TaskConfiguration}, and the set of session
 * partitions before creating a client instance with {@link #build()}.
 *
 * <p>Usage:
 * <pre>{@code
 * ArmoniKClient client = ArmoniKClient.newBuilder()
 *     .withConnectionConfiguration(connectionConfig)
 *     .withDefaultTaskConfiguration(taskConfig)
 *     .withPartition("partition-a")
 *     .build();
 * }</pre>
 *
 * <p>Notes:
 * <ul>
 *   <li>If a default {@link TaskConfiguration} is not provided, the client uses a library default.</li>
 *   <li>Partitions are optional; you may call {@link #withPartition(String)} repeatedly or
 *       {@link #withPartitions(Set)} once to add multiple partitions.</li>
 * </ul>
 */
public class ArmoniKClientBuilder {
  private final Set<String> partitionIds = new HashSet<>();
  private TaskConfiguration taskConfiguration = null;//defaultConfiguration();
  private ArmoniKConnectionConfig connectionConfiguration;

  /**
   * Sets the default task configuration applied to the client's session.
   *
   * @param taskConfiguration the task configuration to use as default (may be {@code null} to use library defaults)
   * @return this builder
   */
  public ArmoniKClientBuilder withDefaultTaskConfiguration(TaskConfiguration taskConfiguration) {
    this.taskConfiguration = taskConfiguration;
    return this;
  }

  /**
   * Sets the connection configuration used by the client.
   *
   * @param connectionConfiguration connection parameters (endpoint, TLS/retry settings)
   * @return this builder
   */
  public ArmoniKClientBuilder withConnectionConfiguration(ArmoniKConnectionConfig connectionConfiguration) {
    this.connectionConfiguration = connectionConfiguration;
    return this;
  }

  /**
   * Adds a single partition identifier to the session configuration.
   *
   * @param partitionId the partition id to add
   * @return this builder
   */
  public ArmoniKClientBuilder withPartition(String partitionId) {
    this.partitionIds.add(partitionId);
    return this;
  }

  /**
   * Adds multiple partition identifiers to the session configuration.
   *
   * @param partitionIds the partition ids to add
   * @return this builder
   * @throws NullPointerException if {@code partitionIds} is {@code null}
   */
  public ArmoniKClientBuilder withPartitions(Set<String> partitionIds) {
    this.partitionIds.addAll(partitionIds);
    return this;
  }

  /**
   * Builds a new {@link ArmoniKClient} instance with the configured settings and opens a session.
   *
   * @return a configured client instance
   * @throws NullPointerException if the connection configuration has not been provided
   */
  public ArmoniKClient build() {
    requireNonNull(connectionConfiguration, "connectionConfiguration must not be null");

    var services = new DefaultServices(connectionConfiguration);
    return new ArmoniKClient(partitionIds, taskConfiguration, services);
  }
}
