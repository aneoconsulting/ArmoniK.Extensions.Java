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
package fr.aneo.armonik.worker.domain.definition.task;

import fr.aneo.armonik.worker.domain.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.blob.OutputBlobDefinition;
import fr.aneo.armonik.worker.domain.*;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Mutable definition of a task to be submitted to the ArmoniK cluster.
 * <p>
 * A {@code TaskDefinition} describes how to create a task including:
 * </p>
 * <ul>
 *   <li>Input blobs - as definitions (data to upload), handles (existing data), or task inputs (parent inputs)</li>
 *   <li>Expected output blobs - names of results the task will produce</li>
 *   <li>Optional task-specific configuration</li>
 * </ul>
 * <p>
 * This class uses a fluent API for building task definitions. Inputs can be provided
 * in three ways:
 * </p>
 * <ul>
 *   <li>{@link InputBlobDefinition} - inline data that will be uploaded during submission</li>
 *   <li>{@link BlobHandle} - reference to existing data in the cluster</li>
 *   <li>{@link TaskInput} - reference to the parent task's input data (reuse)</li>
 * </ul>
 * <p>
 * Outputs can be declared in two ways:
 * </p>
 * <ul>
 *   <li>{@link OutputBlobDefinition} - new output that the task will produce</li>
 *   <li>{@link TaskOutput} - delegation of parent task's output to this subtask</li>
 * </ul>
 *
 * @see InputBlobDefinition
 * @see OutputBlobDefinition
 * @see BlobHandle
 * @see TaskInput
 * @see TaskOutput
 * @see TaskConfiguration
 */
public class TaskDefinition {
  private TaskConfiguration configuration;
  private final Map<String, InputBlobDefinition> inputDefinitions;
  private final Map<String, BlobHandle> inputHandles;
  private final Map<String, TaskInput> taskInputs;
  private final Map<String, OutputBlobDefinition> outputDefinitions;
  private final Map<String, TaskOutput> taskOutputs;

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
    taskInputs = new HashMap<>();
    taskOutputs = new HashMap<>();
  }

  /**
   * Returns an immutable map of input blob definitions keyed by logical name.
   * <p>
   * Input blob definitions contain data that will be uploaded during task submission.
   * These are separate from input handles and task inputs.
   *
   * @return an immutable map of input blob definitions; never {@code null}, may be empty
   * @see #inputHandles()
   * @see #taskInputs()
   * @see #withInput(String, InputBlobDefinition)
   */
  public Map<String, InputBlobDefinition> inputDefinitions() {
    return Map.copyOf(inputDefinitions);
  }

  /**
   * Returns an immutable map of input blob handles keyed by logical name.
   * <p>
   * Input blob handles reference data already stored in the cluster, avoiding
   * duplicate uploads. These are separate from input definitions and task inputs.
   *
   * @return an immutable map of input blob handles; never {@code null}, may be empty
   * @see #inputDefinitions()
   * @see #taskInputs()
   * @see #withInput(String, BlobHandle)
   */
  public Map<String, BlobHandle> inputHandles() {
    return Map.copyOf(inputHandles);
  }

  /**
   * Returns an immutable map of parent task inputs keyed by logical name.
   * <p>
   * Task inputs reference input data from the parent task, allowing subtasks
   * to reuse their parent's inputs without re-uploading or creating new handles.
   * These are separate from input definitions and input handles.
   *
   * @return an immutable map of task inputs; never {@code null}, may be empty
   * @see #inputDefinitions()
   * @see #inputHandles()
   * @see #withInput(String, TaskInput)
   */
  public Map<String, TaskInput> taskInputs() {
    return Map.copyOf(taskInputs);
  }

  /**
   * Converts the task inputs attached to this definition into {@link BlobHandle} instances.
   * <p>
   * Only {@link TaskInput} entries that have been explicitly registered on this
   * {@code TaskDefinition} via {@link #withInput(String, TaskInput)} are exposed here.
   * </p>
   *
   * @return an immutable map of logical input names to their corresponding blob handles
   *         for this definition; never {@code null}, may be empty
   *
   * @see TaskInput
   * @see #taskInputs()
   * @see #inputHandles()
   * @see #withInput(String, TaskInput)
   */
  public Map<String, BlobHandle> taskInputHandles() {
    return taskInputs.entrySet()
                     .stream()
                     .collect(toMap(
                       Map.Entry::getKey,
                       entry -> entry.getValue().asBlobHandle())
                     );
  }

  /**
   * Returns an immutable map of output blob definitions keyed by logical name.
   * <p>
   * Output blob definitions declare new results that the task will produce.
   * They contain only metadata (name), as the actual data will be created
   * during task execution. These are separate from task outputs (delegated outputs).
   *
   * @return an immutable map of output blob definitions; never {@code null}, may be empty
   * @see #taskOutputs()
   * @see #withOutput(String)
   * @see #withOutput(String, OutputBlobDefinition)
   */
  public Map<String, OutputBlobDefinition> outputDefinitions() {
    return Map.copyOf(outputDefinitions);
  }

  /**
   * Returns an immutable map of delegated parent task outputs keyed by logical name.
   * <p>
   * Task outputs represent outputs from the parent task that are delegated to this
   * subtask. The subtask takes over the responsibility of producing these outputs.
   * These are separate from output definitions (new outputs).
   * </p>
   *
   * <h4>Output Delegation Pattern</h4>
   * <p>
   * A parent task can delegate output production to a subtask:
   * </p>
   * <pre>{@code
   * TaskOutput parentOutput = taskContext.getOutput("result");
   *
   * var subtask = new TaskDefinition()
   *     .withInput("data", someData)
   *     .withOutput("result", parentOutput);  // Subtask produces parent's output
   *
   * taskContext.submitTask(subtask);
   * // Parent doesn't write to "result" - subtask will
   * }</pre>
   *
   * @return an immutable map of task outputs; never {@code null}, may be empty
   * @see #outputDefinitions()
   * @see #withOutput(String, TaskOutput)
   */
  public Map<String, TaskOutput> taskOutputs() {
    return Map.copyOf(taskOutputs);
  }

  /**
   * Converts the delegated outputs attached to this definition into {@link BlobHandle} instances.
   * <p>
   * Only {@link TaskOutput} entries that have been explicitly registered on this
   * {@code TaskDefinition} via {@link #withOutput(String, TaskOutput)} are exposed here.
   *
   * @return an immutable map of logical output names to delegated output blob handles
   *         for this definition; never {@code null}, may be empty
   *
   * @see TaskOutput
   * @see #taskOutputs()
   * @see #outputDefinitions()
   * @see #withOutput(String, TaskOutput)
   */
  public Map<String, BlobHandle> taskOutputHandles() {
    return taskOutputs.entrySet()
                      .stream()
                      .collect(toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().asBlobHandle())
                      );
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
   * exists (from a definition, handle, or task input), it will be replaced.
   * </p>
   *
   * @param name                the logical input name, used to reference this input within the task
   * @param inputBlobDefinition the input blob definition containing the input data
   * @return this definition for method chaining
   * @throws NullPointerException     if name or inputBlobDefinition is null
   * @throws IllegalArgumentException if name is blank
   * @see InputBlobDefinition
   * @see #withInput(String, BlobHandle)
   * @see #withInput(String, TaskInput)
   */
  public TaskDefinition withInput(String name, InputBlobDefinition inputBlobDefinition) {
    requireNonNull(inputBlobDefinition, "inputBlobDefinition must not be null");
    validateName(name);

    inputDefinitions.put(name, inputBlobDefinition);
    inputHandles.remove(name);
    taskInputs.remove(name);
    return this;
  }

  /**
   * Adds or replaces an input by referencing an existing blob in the ArmoniK cluster.
   * <p>
   * The blob handle references data that is already stored in the cluster, avoiding
   * the need to upload the same data multiple times. If an input with the same name
   * already exists (from a definition, handle, or task input), it will be replaced.
   * </p>
   *
   * @param name       the logical input name, used to reference this input within the task
   * @param blobHandle the handle of an existing blob to use as input
   * @return this definition for method chaining
   * @throws NullPointerException     if name or blobHandle is null
   * @throws IllegalArgumentException if name is blank
   * @see BlobHandle
   * @see #withInput(String, InputBlobDefinition)
   * @see #withInput(String, TaskInput)
   */
  public TaskDefinition withInput(String name, BlobHandle blobHandle) {
    requireNonNull(blobHandle, "blobHandle must not be null");
    validateName(name);

    inputHandles.put(name, blobHandle);
    inputDefinitions.remove(name);
    taskInputs.remove(name);
    return this;
  }

  /**
   * Adds or replaces an input by reusing the parent task's input data.
   * <p>
   * This method allows a subtask to reference an input from its parent task without
   * re-uploading or creating a new blob handle. The subtask will have access to the same
   * data that the parent task received. If an input with the same name already exists
   * (from a definition, handle, or task input), it will be replaced.
   * </p>
   *
   * <h4>Use Case</h4>
   * <p>
   * When creating subtasks that need access to the parent's input data:
   * </p>
   * <pre>{@code
   * // In parent task processor
   * TaskInput configInput = taskContext.getInput("config");
   *
   * var subtask = new TaskDefinition()
   *     .withInput("config", configInput)  // Reuse parent's input
   *     .withOutput("processedData");
   *
   * taskContext.submitTask(subtask);
   * }</pre>
   *
   * @param name      the logical input name for the subtask
   * @param taskInput the parent task's input to reuse
   * @return this definition for method chaining
   * @throws NullPointerException     if name or taskInput is null
   * @throws IllegalArgumentException if name is blank
   * @see TaskInput
   * @see #withInput(String, InputBlobDefinition)
   * @see #withInput(String, BlobHandle)
   */
  public TaskDefinition withInput(String name, TaskInput taskInput) {
    requireNonNull(taskInput, "taskInput must not be null");
    validateName(name);

    taskInputs.put(name, taskInput);
    inputHandles.remove(name);
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
   * @throws NullPointerException     if name is null
   * @throws IllegalArgumentException if name is blank
   * @see TaskHandle#outputs()
   * @see #withOutput(String, OutputBlobDefinition)
   * @see #withOutput(String, TaskOutput)
   */
  public TaskDefinition withOutput(String name) {
    return withOutput(name, OutputBlobDefinition.create());
  }

  /**
   * Declares an expected output with an explicit output blob definition.
   * <p>
   * The output blob definition contains metadata (name) for the result that the task
   * will produce. The actual data will be created during task execution.
   * If an output with the same name already exists (from a definition or task output),
   * it will be replaced.
   * </p>
   * <p>
   * After task submission, the logical name will correspond to a blob handle available
   * through {@link TaskHandle#outputs()}.
   * </p>
   *
   * @param name                 the logical output name
   * @param outputBlobDefinition the output blob definition specifying result metadata
   * @return this definition for method chaining
   * @throws NullPointerException     if name or outputBlobDefinition is null
   * @throws IllegalArgumentException if name is blank
   * @see OutputBlobDefinition
   * @see TaskHandle#outputs()
   * @see #withOutput(String, TaskOutput)
   */
  public TaskDefinition withOutput(String name, OutputBlobDefinition outputBlobDefinition) {
    requireNonNull(outputBlobDefinition, "outputBlobDefinition must not be null");
    validateName(name);

    outputDefinitions.put(name, outputBlobDefinition);
    taskOutputs.remove(name);
    return this;
  }

  /**
   * Adds or replaces an output by delegating its production to this subtask.
   * <p>
   * <strong>Output Delegation:</strong> This method enables a pattern where a parent task
   * can delegate the responsibility of producing one of its expected outputs to a subtask.
   * The subtask will produce the data, and the parent task's callers will receive it without
   * knowing about the delegation. If an output with the same name already exists (from a
   * definition or task output), it will be replaced.
   *
   * @param name       the logical output name for the subtask
   * @param taskOutput the output from the parent task to delegate
   * @return this definition for method chaining
   * @throws NullPointerException     if name or taskOutput is null
   * @throws IllegalArgumentException if name is blank
   * @see TaskOutput
   * @see #withOutput(String)
   * @see #withOutput(String, OutputBlobDefinition)
   */
  public TaskDefinition withOutput(String name, TaskOutput taskOutput) {
    requireNonNull(taskOutput, "taskOutput must not be null");
    validateName(name);

    taskOutputs.put(name, taskOutput);
    outputDefinitions.remove(name);
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
      ", taskInputs=" + taskInputs.keySet() +
      ", outputDefinitions=" + outputDefinitions.keySet() +
      ", taskOutputs=" + taskOutputs.keySet() +
      ", configuration=" + configuration +
      ']';
  }
}
