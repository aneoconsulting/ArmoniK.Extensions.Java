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
package fr.aneo.armonik.client.task;

import fr.aneo.armonik.client.blob.BlobDefinition;
import fr.aneo.armonik.client.blob.BlobHandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Builder-style description of a task to submit to ArmoniK.
 * <p>
 * A {@code TaskDefinition} lets you declare input payloads (inline bytes), reference existing blob inputs,
 * list expected outputs by name, and optionally attach a {@link TaskConfiguration}.
 *
 * <p>Inputs can be provided in two ways:
 * <ul>
 *   <li>Inline payloads via {@link #withInput(String, BlobDefinition)}.</li>
 *   <li>References to already-existing blobs via {@link #withInput(String, BlobHandle)}.</li>
 * </ul>
 * If both an inline payload and a blob reference are added under the same name, the latest call wins.
 */
public class TaskDefinition {
  private TaskConfiguration configuration;
  private final Map<String, BlobDefinition> inputDefinitions;
  private final Map<String, BlobHandle> inputHandles;
  private final List<String> outputs = new ArrayList<>();


  /**
   * Creates an empty task definition with no inputs or outputs.
   * Inline inputs and blob inputs can be added afterward, and outputs can be declared by name.
   */
  public TaskDefinition() {
    inputDefinitions = new HashMap<>();
    inputHandles = new HashMap<>();
  }

  /**
   * Returns an immutable view of input definitions, keyed by their logical names.
   * <p>
   * This map includes only inputs provided via {@link #withInput(String, BlobDefinition)}.
   * </p>
   *
   * @return an immutable map of input definitions
   */

  public Map<String, BlobDefinition> inputs() {
    return Map.copyOf(inputDefinitions);
  }

  /**
   * Returns an immutable view of inputs referenced by existing blob handles.
   * <p>
   * This map includes only inputs provided via {@link #withInput(String, BlobHandle)}.
   * </p>
   *
   * @return an immutable map of input handles
   */
  public Map<String, BlobHandle> inputHandles() {
    return Map.copyOf(inputHandles);
  }

  /**
   * Returns a list of declared output names.
   *
   * @return an immutable list of output names (may be empty)
   */
  public List<String> outputs() {
    return List.copyOf(outputs);
  }

  /**
   * Returns the task configuration associated with this definition, or {@code null} if not set.
   *
   * @return the configuration, or {@code null}
   */
  public TaskConfiguration configuration() {
    return configuration;
  }

  /**
   * Sets the task configuration to be applied on submission.
   *
   * @param taskConfiguration the configuration to use
   * @return this definition for method chaining
   */
  public TaskDefinition withConfiguration(TaskConfiguration taskConfiguration) {
    this.configuration = taskConfiguration;
    return this;
  }

  /**
   * Adds or replaces an input under the given logical name using a {@link BlobDefinition}.
   * <p>
   * If an input with the same {@code name} already exists (definition or handle),
   * this definition becomes the effective input for that name.
   * </p>
   *
   * @param name           the logical input name (non-blank)
   * @param blobDefinition the blob source definition to use
   * @return this definition for method chaining
   * @throws NullPointerException     if {@code name} or {@code blobDefinition} is {@code null}
   * @throws IllegalArgumentException if {@code name} is blank
   */
  public TaskDefinition withInput(String name, BlobDefinition blobDefinition) {
    validateName(name);
    inputDefinitions.put(name, blobDefinition);
    inputHandles.remove(name);
    return this;
  }

  /**
   * Adds or replaces an input under the given logical name by referencing an existing blob.
   * <p>
   * If an input with the same {@code name} already exists (definition or handle),
   * this handle becomes the effective input for that name.
   * </p>
   *
   * @param name       the logical input name (non-blank)
   * @param blobHandle the handle of an existing blob to use as input
   * @return this definition for method chaining
   * @throws NullPointerException     if {@code name} or {@code blobHandle} is {@code null}
   * @throws IllegalArgumentException if {@code name} is blank
   */
  public TaskDefinition withInput(String name, BlobHandle blobHandle) {
    validateName(name);
    inputHandles.put(name, blobHandle);
    inputDefinitions.remove(name);
    return this;
  }

  /**
   * Declares an expected output by its logical name.
   *
   * @param name the output name (non-blank)
   * @return this definition for method chaining
   * @throws NullPointerException     if {@code name} is {@code null}
   * @throws IllegalArgumentException if {@code name} is blank
   */
  public TaskDefinition withOutput(String name) {
    validateName(name);
    outputs.add(name);
    return this;
  }

  private static void validateName(String name) {
    if (name == null) throw new NullPointerException("name must not be null");
    if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
  }
}
