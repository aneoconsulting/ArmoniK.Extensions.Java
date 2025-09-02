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
import java.util.UUID;

import static java.util.Objects.requireNonNull;


/**
 * Immutable handle to an ArmoniK session.
 * <p>
 * A session groups task submissions under a common scope, including the set of
 * target partitions and a default {@link TaskConfiguration} used for tasks
 * unless explicitly overridden per task.
 * </p>
 */
public final class Session {

  private final UUID id;
  private final Set<String> partitions;
  private final TaskConfiguration defaultTaskConfiguration;


  /**
   * Creates a new session instance.
   *
   * @param id                       unique session identifier
   * @param partitions               set of partition identifiers associated with this session;
   *                                 if {@code null}, an empty set is used
   * @param defaultTaskConfiguration default task configuration applied when submitting tasks
   *                                 through this session.
   * @throws NullPointerException if {@code id} is {@code null}
   */
  Session(
    UUID id,
    Set<String> partitions,
    TaskConfiguration defaultTaskConfiguration
  ) {
    requireNonNull(id, "id");
    this.partitions = partitions == null ? Set.of() : partitions;
    this.defaultTaskConfiguration = defaultTaskConfiguration;
    this.id = id;
  }

  /**
   * Returns the session identifier.
   * @return the session identifier
   */
  public UUID id() {
    return id;
  }

  /**
   * Returns the set of partition identifiers associated with this session.
   * @return the set of partition identifiers
   */
  public Set<String> partitions() {
    return partitions;
  }

  /**
   * Returns the default task configuration applied when submitting tasks through this session.
   * @return the default task configuration
   */
  public TaskConfiguration defaultTaskConfiguration() {
    return defaultTaskConfiguration;
  }

  @Override
  public String toString() {
    return "Session[" +
      "id=" + id + ", " +
      "partitions=" + partitions + ", " +
      "defaultTaskConfiguration=" + defaultTaskConfiguration + ']';
  }
}
