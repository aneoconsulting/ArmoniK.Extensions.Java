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

import fr.aneo.armonik.client.model.BlobHandle;
import fr.aneo.armonik.client.model.SessionHandle;
import fr.aneo.armonik.client.model.TaskConfiguration;
import fr.aneo.armonik.client.model.TaskHandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Builder-style definition for describing a task to submit to the ArmoniK distributed computing platform.
 * <p>
 * A {@code TaskDefinition} allows you to specify task inputs, declare expected outputs, and configure
 * task-specific execution parameters. This definition is used by {@link SessionHandle#submitTask(TaskDefinition)}
 * to create and submit tasks for execution in the ArmoniK cluster.
 * <p>
 * Inputs can be provided in two ways:
 * <ul>
 *   <li><strong>Inline data:</strong> Direct byte content via {@link #withInput(String, BlobDefinition)}</li>
 *   <li><strong>Existing blobs:</strong> References to already-stored blobs via {@link #withInput(String, BlobHandle)}</li>
 * </ul>
 * If both an inline definition and a blob reference are added under the same name, the latest call takes precedence.
 * <p>
 * This class follows the builder pattern and supports method chaining for fluent configuration.
 *
 * @see SessionHandle#submitTask(TaskDefinition)
 * @see TaskHandle
 * @see BlobDefinition
 * @see BlobHandle
 * @see TaskConfiguration
 */
public class TaskDefinition {
  private TaskConfiguration configuration;
  private final Map<String, BlobDefinition> inputDefinitions;
  private final Map<String, BlobHandle> inputHandles;
  private final List<String> outputs = new ArrayList<>();


  /**
   * Creates an empty task definition with no inputs, outputs, or configuration.
   * <p>
   * Inputs, outputs, and configuration can be added using the fluent builder methods
   * {@link #withInput(String, BlobDefinition)}, {@link #withInput(String, BlobHandle)},
   * {@link #withOutput(String)}, and {@link #withConfiguration(TaskConfiguration)}.
   */
  public TaskDefinition() {
    inputDefinitions = new HashMap<>();
    inputHandles = new HashMap<>();
  }

  /**
   * Returns an immutable view of input definitions provided as inline data.
   * <p>
   * This map includes only inputs added via {@link #withInput(String, BlobDefinition)} and
   * is keyed by the logical input names. Inputs added via {@link #withInput(String, BlobHandle)}
   * are available through {@link #inputHandles()}.
   *
   * @return an immutable map of input definitions, keyed by input name
   * @see #inputHandles()
   * @see BlobDefinition
   */
  public Map<String, BlobDefinition> inputDefinitions() {
    return Map.copyOf(inputDefinitions);
  }

  /**
   * Returns an immutable view of inputs referenced by existing blob handles.
   * <p>
   * This map includes only inputs added via {@link #withInput(String, BlobHandle)} and
   * is keyed by the logical input names. Inputs added via {@link #withInput(String, BlobDefinition)}
   * are available through {@link #inputDefinitions()}.
   *
   * @return an immutable map of input handles, keyed by input name
   * @see #inputDefinitions()
   * @see BlobHandle
   */
  public Map<String, BlobHandle> inputHandles() {
    return Map.copyOf(inputHandles);
  }

  /**
   * Returns an immutable list of declared output names.
   * <p>
   * Output names specify the expected results that the task will produce. These names
   * correspond to the blob handles available in {@link TaskHandle#outputs()} after
   * task submission.
   *
   * @return an immutable list of output names, may be empty if no outputs are expected
   * @see TaskHandle#outputs()
   */
  public List<String> outputs() {
    return List.copyOf(outputs);
  }

  /**
   * Returns the task configuration associated with this definition.
   * <p>
   * The task configuration defines execution parameters such as priority, retry count,
   * and resource requirements. If no task-specific configuration is set, this method
   * returns {@code null}, indicating that session-level defaults will be applied.
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
   * Adds or replaces an input using inline data provided through a blob definition.
   * <p>
   * The blob definition contains the actual data content that will be uploaded to the
   * ArmoniK cluster as part of task submission. If an input with the same name already
   * exists (either from a previous definition or handle), it will be replaced.
   *
   * @param name the logical input name, used to reference this input within the task
   * @param blobDefinition the blob definition containing the input data
   * @return this definition for method chaining
   * @throws NullPointerException if name or blobDefinition is null
   * @throws IllegalArgumentException if name is blank
   * @see BlobDefinition
   * @see #withInput(String, BlobHandle)
   */
  public TaskDefinition withInput(String name, BlobDefinition blobDefinition) {
    validateName(name);
    inputDefinitions.put(name, blobDefinition);
    inputHandles.remove(name);
    return this;
  }

  /**
   * Adds or replaces an input by referencing an existing blob in the ArmoniK cluster.
   * <p>
   * The blob handle references data that is already stored in the cluster, avoiding
   * the need to upload the same data multiple times. If an input with the same name
   * already exists (either from a previous definition or handle), it will be replaced.
   *
   * @param name the logical input name, used to reference this input within the task
   * @param blobHandle the handle of an existing blob to use as input
   * @return this definition for method chaining
   * @throws NullPointerException if name or blobHandle is null
   * @throws IllegalArgumentException if name is blank
   * @see BlobHandle
   * @see #withInput(String, BlobDefinition)
   */
  public TaskDefinition withInput(String name, BlobHandle blobHandle) {
    validateName(name);
    inputHandles.put(name, blobHandle);
    inputDefinitions.remove(name);
    return this;
  }

  /**
   * Declares an expected output by its logical name.
   * <p>
   * Output names specify the results that the task is expected to produce during execution.
   * After task submission, these names will correspond to blob handles available through
   * {@link TaskHandle#outputs()}.
   *
   * @param name the logical output name
   * @return this definition for method chaining
   * @throws NullPointerException if name is null
   * @throws IllegalArgumentException if name is blank
   * @see TaskHandle#outputs()
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
