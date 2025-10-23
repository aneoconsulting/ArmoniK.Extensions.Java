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
package fr.aneo.armonik.worker.definition.task;

import fr.aneo.armonik.worker.BlobHandle;
import fr.aneo.armonik.worker.TaskConfiguration;
import fr.aneo.armonik.worker.TaskHandle;
import fr.aneo.armonik.worker.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.definition.blob.OutputBlobDefinition;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Mutable definition of a task to be submitted to the ArmoniK cluster.
 * <p>
 * A {@code TaskDefinition} describes how to create a task including:
 * </p>
 * <ul>
 *   <li>Input blobs - either as definitions (data to upload) or handles (existing data)</li>
 *   <li>Expected output blobs - names of results the task will produce</li>
 *   <li>Optional task-specific configuration</li>
 * </ul>
 * <p>
 * This class uses a fluent API for building task definitions. Inputs can be provided
 * in two ways:
 * </p>
 * <ul>
 *   <li>{@link InputBlobDefinition} - inline data that will be uploaded during submission</li>
 *   <li>{@link BlobHandle} - reference to existing data in the cluster</li>
 * </ul>
 * <p>
 * Outputs are declared using {@link OutputBlobDefinition}, which contains only metadata
 * (name). The actual data will be produced by the task during execution.
 * </p>
 *
 * @see InputBlobDefinition
 * @see OutputBlobDefinition
 * @see BlobHandle
 * @see TaskConfiguration
 */
public class TaskDefinition {
  private TaskConfiguration configuration;
  private final Map<String, InputBlobDefinition> inputDefinitions;
  private final Map<String, BlobHandle> inputHandles;
  private final Map<String, OutputBlobDefinition> outputDefinitions;

  /**
   * Creates a new empty task definition.
   * <p>
   * Use the fluent API methods to configure the task before submission.
   * </p>
   */
  public TaskDefinition() {
    inputDefinitions = new HashMap<>();
    inputHandles = new HashMap<>();
    outputDefinitions = new HashMap<>();
  }

  /**
   * Returns an immutable map of input blob definitions keyed by logical name.
   * <p>
   * Input blob definitions contain data that will be uploaded during task submission.
   * These are separate from input handles, which reference existing data.
   * </p>
   *
   * @return an immutable map of input blob definitions; never {@code null}, may be empty
   * @see #inputHandles()
   * @see #withInput(String, InputBlobDefinition)
   */
  public Map<String, InputBlobDefinition> inputDefinitions() {
    return Map.copyOf(inputDefinitions);
  }

  /**
   * Returns an immutable map of input blob handles keyed by logical name.
   * <p>
   * Input blob handles reference data already stored in the cluster, avoiding
   * duplicate uploads. These are separate from input definitions, which contain
   * data to be uploaded.
   * </p>
   *
   * @return an immutable map of input blob handles; never {@code null}, may be empty
   * @see #inputDefinitions()
   * @see #withInput(String, BlobHandle)
   */
  public Map<String, BlobHandle> inputHandles() {
    return Map.copyOf(inputHandles);
  }

  /**
   * Returns an immutable map of output blob definitions keyed by logical name.
   * <p>
   * Output blob definitions declare the results that the task will produce.
   * They contain only metadata (name), as the actual data will be created
   * during task execution.
   * </p>
   *
   * @return an immutable map of output blob definitions; never {@code null}, may be empty
   * @see #withOutput(String)
   * @see #withOutput(String, OutputBlobDefinition)
   */
  public Map<String, OutputBlobDefinition> outputDefinitions() {
    return Map.copyOf(outputDefinitions);
  }

  /**
   * Returns the task configuration associated with this definition.
   * <p>
   * The task configuration defines execution parameters such as priority, retry count,
   * and resource requirements. If no task-specific configuration is set, this method
   * returns {@code null}, indicating that session-level defaults will be applied.
   * </p>
   *
   * @return the task configuration, or {@code null} if using session defaults
   * @see TaskConfiguration
   * @see #withConfiguration(TaskConfiguration)
   */
  public TaskConfiguration configuration() {
    return configuration;
  }

  /**
   * Sets the task configuration to be applied during task execution.
   * <p>
   * The task configuration overrides session-level defaults for execution parameters
   * such as priority, retry count, partition targeting, and resource limits.
   * </p>
   *
   * @param taskConfiguration the configuration to use for this task
   * @return this definition for method chaining
   * @throws NullPointerException if taskConfiguration is null
   * @see TaskConfiguration
   */
  public TaskDefinition withConfiguration(TaskConfiguration taskConfiguration) {
    this.configuration = taskConfiguration;
    return this;
  }

  /**
   * Adds or replaces an input using inline data provided through an input blob definition.
   * <p>
   * The input blob definition contains the actual data content that will be uploaded to
   * the ArmoniK cluster as part of task submission. If an input with the same name already
   * exists (either from a previous definition or handle), it will be replaced.
   * </p>
   *
   * @param name the logical input name, used to reference this input within the task
   * @param inputBlobDefinition the input blob definition containing the input data
   * @return this definition for method chaining
   * @throws NullPointerException if name or inputBlobDefinition is null
   * @throws IllegalArgumentException if name is blank
   * @see InputBlobDefinition
   * @see #withInput(String, BlobHandle)
   */
  public TaskDefinition withInput(String name, InputBlobDefinition inputBlobDefinition) {
    requireNonNull(inputBlobDefinition, "inputBlobDefinition must not be null");
    validateName(name);

    inputDefinitions.put(name, inputBlobDefinition);
    inputHandles.remove(name);
    return this;
  }

  /**
   * Adds or replaces an input by referencing an existing blob in the ArmoniK cluster.
   * <p>
   * The blob handle references data that is already stored in the cluster, avoiding
   * the need to upload the same data multiple times. If an input with the same name
   * already exists (either from a previous definition or handle), it will be replaced.
   * </p>
   *
   * @param name the logical input name, used to reference this input within the task
   * @param blobHandle the handle of an existing blob to use as input
   * @return this definition for method chaining
   * @throws NullPointerException if name or blobHandle is null
   * @throws IllegalArgumentException if name is blank
   * @see BlobHandle
   * @see #withInput(String, InputBlobDefinition)
   */
  public TaskDefinition withInput(String name, BlobHandle blobHandle) {
    requireNonNull(blobHandle, "blobHandle must not be null");
    validateName(name);

    inputHandles.put(name, blobHandle);
    inputDefinitions.remove(name);
    return this;
  }

  /**
   * Declares an expected output by its logical name using an empty-named output blob definition.
   * <p>
   * This is a convenience method equivalent to {@code withOutput(name, OutputBlobDefinition.create())}.
   * The output will have an empty name in ArmoniK's metadata, and the server will generate
   * a unique ID for it.
   * </p>
   * <p>
   * Output names specify the results that the task is expected to produce during execution.
   * After task submission, these names will correspond to blob handles available through
   * {@link TaskHandle#outputs()}.
   * </p>
   *
   * @param name the logical output name
   * @return this definition for method chaining
   * @throws NullPointerException if name is null
   * @throws IllegalArgumentException if name is blank
   * @see TaskHandle#outputs()
   * @see #withOutput(String, OutputBlobDefinition)
   */
  public TaskDefinition withOutput(String name) {
    return withOutput(name, OutputBlobDefinition.create());
  }

  /**
   * Declares an expected output with an explicit output blob definition.
   * <p>
   * The output blob definition contains metadata (name) for the result that the task
   * will produce. The actual data will be created during task execution.
   * </p>
   * <p>
   * After task submission, the logical name will correspond to a blob handle available
   * through {@link TaskHandle#outputs()}.
   * </p>
   *
   * @param name the logical output name
   * @param outputBlobDefinition the output blob definition specifying result metadata
   * @return this definition for method chaining
   * @throws NullPointerException if name or outputBlobDefinition is null
   * @throws IllegalArgumentException if name is blank
   * @see OutputBlobDefinition
   * @see TaskHandle#outputs()
   */
  public TaskDefinition withOutput(String name, OutputBlobDefinition outputBlobDefinition) {
    requireNonNull(outputBlobDefinition, "outputBlobDefinition must not be null");
    validateName(name);

    outputDefinitions.put(name, outputBlobDefinition);
    return this;
  }

  private static void validateName(String name) {
    requireNonNull(name, "name must not be null");
    if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
  }

  @Override
  public String toString() {
    return "TaskDefinition[" +
      "inputDefinitions=" + inputDefinitions.keySet() +
      ", inputHandles=" + inputHandles.keySet() +
      ", outputDefinitions=" + outputDefinitions.keySet() +
      ", configuration=" + configuration +
      ']';
  }
}
