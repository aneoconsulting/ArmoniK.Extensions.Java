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
package fr.aneo.armonik.client.definition.blob;

import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.BlobHandle;

import static java.util.Objects.requireNonNullElse;

/**
 * Definition of expected output data to be produced by a task in the ArmoniK cluster.
 * <p>
 * An {@code OutputBlobDefinition} declares that a task will produce a blob with specific
 * properties (name and deletion policy), but does not contain the actual data content.
 * The data will be generated during task execution by the worker and uploaded to the cluster.
 * <p>
 * <strong>Usage Examples:</strong>
 * <pre>{@code
 * // Simple output with empty monitoring name (common for temporary results)
 * task.withOutput("result");
 *
 * // Output with meaningful monitoring name for observability
 * task.withOutput("result", OutputBlobDefinition.from("model_weights_v2", false));
 *
 * // Output requiring manual deletion (persistent data)
 * task.withOutput("checkpoint", OutputBlobDefinition.from("training_checkpoint", true));
 * }</pre>
 *
 * @see InputBlobDefinition
 * @see BlobDefinition
 * @see BlobHandle
 * @see TaskDefinition
 */
public final class OutputBlobDefinition implements BlobDefinition {

  private final String name;
  private final boolean manualDeletion;

  /**
   * Creates an output blob definition with the specified properties.
   * <p>
   * If the provided name is {@code null}, it is converted to an empty string.
   * <p>
   * Use factory methods like {@link #from(String)} or {@link #from(String, boolean)}
   * for more convenient creation.
   *
   * @param name           the user-defined name for this output blob, or null for empty string
   * @param manualDeletion whether the user is responsible for deleting the data
   */
  private OutputBlobDefinition(String name, boolean manualDeletion) {
    this.name = requireNonNullElse(name, "");
    this.manualDeletion = manualDeletion;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean manualDeletion() {
    return manualDeletion;
  }

  /**
   * Creates an output blob definition with the specified name and deletion policy.
   * <p>
   * If the provided name is {@code null}, it is converted to an empty string.
   *
   * @param name           the user-defined name for this output blob, or null for empty string
   * @param manualDeletion whether the user is responsible for deleting the data
   * @return a new output blob definition
   */
  public static OutputBlobDefinition from(String name, boolean manualDeletion) {
    return new OutputBlobDefinition(name, manualDeletion);
  }

  /**
   * Creates an output blob definition with the specified name and automatic deletion.
   * <p>
   * This convenience factory method creates an output blob with:
   * <ul>
   *   <li>The specified name</li>
   *   <li>Automatic deletion management (manualDeletion = false)</li>
   * </ul>
   * <p>
   * If the provided name is {@code null}, it is converted to an empty string.
   * This is the most common way to create output blob definitions for temporary
   * results that don't need to persist beyond the session.
   *
   * @param name the user-defined name for this output blob, or null for empty string
   * @return a new output blob definition with automatic deletion
   * @see #from(String, boolean)
   */
  public static OutputBlobDefinition from(String name) {
    return from(name, false);
  }
}
