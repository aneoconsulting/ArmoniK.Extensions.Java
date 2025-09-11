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
package fr.aneo.armonik.client.session;

import fr.aneo.armonik.client.task.TaskConfiguration;

import java.util.Set;

/**
 * Service API to create ArmoniK sessions.
 * <p>
 * A session defines the execution scope for submitted tasks, including targeted partitions
 * and a default {@link TaskConfiguration}.
 * </p>
 *
 * @see SessionHandle
 * @see TaskConfiguration
 */

public interface SessionService {

  /**
   * Creates a session associated with the given partitions using the default task configuration.
   * <p>
   * The default configuration is typically equivalent to {@code TaskConfiguration.defaultConfiguration()}.
   * </p>
   *
   * @param partitionIds the set of partition identifiers to associate with the session (may be empty, but not null)
   * @return a newly created {@link SessionHandle}
   * @throws NullPointerException     if {@code partitionIds} is {@code null}
   * @throws IllegalArgumentException if the effective default configuration targets a partition
   *                                  not present in {@code partitionIds}
   */

  SessionHandle createSession(Set<String> partitionIds);

  /**
   * Creates a session associated with the given partitions and an explicit default task configuration.
   *
   * @param partitionIds      the set of partition identifiers to associate with the session (may be empty, but not null)
   * @param taskConfiguration the default task configuration to apply for tasks submitted in this session;
   *                          if {@code null}, {@code TaskConfiguration.defaultConfiguration()} is applied
   * @return a newly created {@link SessionHandle}
   * @throws NullPointerException     if {@code partitionIds} is {@code null}
   * @throws IllegalArgumentException if {@code taskConfiguration} targets a partition not listed in {@code partitionIds}
   */
  SessionHandle createSession(Set<String> partitionIds, TaskConfiguration taskConfiguration);
}
