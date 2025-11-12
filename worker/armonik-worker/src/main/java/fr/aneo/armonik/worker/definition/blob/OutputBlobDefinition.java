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
package fr.aneo.armonik.worker.definition.blob;

import static java.util.Objects.requireNonNull;

/**
 * Definition of an output blob metadata without associated data.
 * <p>
 * An {@code OutputBlobDefinition} represents a blob that will be produced by a task.
 * It contains only the blob name, as the actual data will be provided later by the
 * task execution.
 * </p>
 * <p>
 * Output blobs are used as:
 * </p>
 * <ul>
 *   <li>Expected outputs of tasks</li>
 *   <li>Results that tasks promise to produce</li>
 * </ul>
 * <p>
 * The ArmoniK cluster creates metadata for these blobs, allowing downstream tasks
 * to depend on them before they are actually produced.
 * </p>
 *
 * @see BlobDefinition
 * @see InputBlobDefinition
 */
public final class OutputBlobDefinition implements BlobDefinition {
  private final String name;

  /**
   * Package-private constructor to enforce factory method usage.
   *
   * @param name the blob name; must not be {@code null}
   */
  OutputBlobDefinition(String name) {
    this.name = requireNonNull(name, "name cannot be null");
  }

  @Override
  public String name() {
    return name;
  }

  /**
   * Creates an output blob definition with an explicit name.
   * <p>
   * The output blob will be created as metadata in ArmoniK's object storage.
   * The actual data will be provided later by the task that produces this output.
   * </p>
   *
   * @param name the blob name for this blob in ArmoniK; must not be {@code null}
   * @return an output blob definition with the given name; never {@code null}
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static OutputBlobDefinition from(String name) {
    requireNonNull(name, "name cannot be null");
    return new OutputBlobDefinition(name);
  }

  /**
   * Creates an output blob definition without an explicit name.
   * <p>
   * The blob name will be set to an empty string. The ArmoniK server will generate
   * a unique ID for the blob regardless of the name.
   * </p>
   *
   * @return an output blob definition with an empty name; never {@code null}
   */
  public static OutputBlobDefinition create() {
    return new OutputBlobDefinition("");
  }

  @Override
  public String toString() {
    return "OutputBlobDefinition[name='" + name + "']";
  }
}
