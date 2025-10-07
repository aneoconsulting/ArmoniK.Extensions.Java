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
package fr.aneo.armonik.client.model;

import fr.aneo.armonik.client.definition.TaskDefinition;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Handle representing a submitted task within the ArmoniK distributed computing platform.
 * <p>
 * A task in ArmoniK is a unit of work launched by a client and processed by a worker [[1]](https://armonik.readthedocs.io/en/latest/content/armonik/glossary.html).
 * Tasks cannot communicate with each other directly, but can depend on each other through their inputs and outputs.
 * This handle provides access to task metadata, configuration, and associated blob handles for inputs and outputs.
 * <p>
 * TaskHandle instances are immutable references that:
 * <ul>
 *   <li>Provide access to the session context where the task was submitted</li>
 *   <li>Hold references to input and output blob handles</li>
 *   <li>Expose task configuration and metadata asynchronously</li>
 *   <li>Support non-blocking access to task information</li>
 * </ul>
 * <p>
 * The handle itself does not perform blocking operations. Task information and blob operations
 * are accessed through completion stages and blob handles, respectively.
 *
 * @see TaskInfo
 * @see TaskConfiguration
 * @see BlobHandle
 * @see SessionHandle
 */
public final class TaskHandle {
  private final SessionId sessionId;
  private final TaskConfiguration configuration;
  private final CompletionStage<TaskInfo> deferredTaskInfo;
  private final Map<String, BlobHandle> inputs;
  private final Map<String, BlobHandle> outputs;

  /**
   * Creates a new task handle with the specified task context and associated resources.
   * <p>
   * This package-private constructor is used internally during task submission to create
   * handles that reference the submitted task and its associated blobs within the session context.
   *
   * @param sessionId        the identifier of the session in which this task was submitted
   * @param configuration    the configuration applied to this task, may be {@code null} if using session defaults
   * @param deferredTaskInfo a completion stage that will provide task metadata when available
   * @param inputs           a map of input blob handles associated with this task, keyed by input name
   * @param outputs          a map of expected output blob handles associated with this task, keyed by output name
   * @throws NullPointerException if sessionId, deferredTaskInfo, inputs, or outputs are null
   * @see TaskInfo
   * @see BlobHandle
   */
  TaskHandle(
    SessionId sessionId,
    TaskConfiguration configuration,
    CompletionStage<TaskInfo> deferredTaskInfo,
    Map<String, BlobHandle> inputs,
    Map<String, BlobHandle> outputs
  ) {
    this.sessionId = sessionId;
    this.configuration = configuration;
    this.deferredTaskInfo = deferredTaskInfo;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  /**
   * Returns the identifier of the session in which this task was submitted.
   * <p>
   * All tasks belong to a session context that defines their execution environment
   * and resource allocation settings.
   *
   * @return the session identifier
   * @see SessionId
   * @see SessionHandle
   */
  public SessionId sessionId() {
    return sessionId;
  }

  /**
   * Returns the configuration applied to this task.
   * <p>
   * The task configuration defines execution parameters such as priority, maximum duration,
   * resource requirements, and other task-specific settings. This configuration follows
   * a hierarchy where task-specific configuration takes precedence over session-level defaults:
   * <ul>
   *   <li>If a task-specific configuration was provided during {@link SessionHandle#submitTask(TaskDefinition)},
   *       that configuration is returned</li>
   *   <li>If no task-specific configuration was provided, the session-level configuration is returned</li>
   * </ul>
   * This method never returns {@code null} as there is always either a task-specific or
   * session-level configuration available.
   *
   * @return the effective task configuration (either task-specific or session-level)
   * @see TaskConfiguration
   * @see SessionHandle#submitTask(TaskDefinition)
   */
  public TaskConfiguration taskConfiguration() {
    return configuration;
  }

  /**
   * Returns a completion stage that provides immutable metadata about this task.
   * <p>
   * The task information becomes available after the ArmoniK cluster acknowledges
   * the task submission and assigns identifiers. The returned completion stage
   * may complete exceptionally if task submission or metadata retrieval fails.
   * <p>
   * This method provides non-blocking access to task metadata, including the
   * task identifier, status information, and other cluster-assigned properties.
   *
   * @return a completion stage that completes with task metadata
   * @see TaskInfo
   */
  public CompletionStage<TaskInfo> deferredTaskInfo() {
    return deferredTaskInfo;
  }

  /**
   * Returns an immutable map of input blob handles used by this task.
   * <p>
   * Input blobs contain the data required for task execution. Each blob is identified
   * by a string key that corresponds to the input names specified in the task definition.
   * The returned map is a defensive copy, and modifications will not affect the original.
   *
   * @return an immutable map of input blob handles, keyed by input name
   * @see BlobHandle
   */
  public Map<String, BlobHandle> inputs() {
    return Map.copyOf(inputs);
  }

  /**
   * Returns an immutable map of expected output blob handles for this task.
   * <p>
   * Output blobs represent the results that will be produced by task execution.
   * Each blob is identified by a string key that corresponds to the output names
   * declared in the task definition. The returned map is a defensive copy, and
   * modifications will not affect the original.
   * <p>
   * Output blob handles can be used to download result data once the task completes
   * and produces the expected outputs.
   *
   * @return an immutable map of output blob handles, keyed by output name
   * @see BlobHandle
   */
  public Map<String, BlobHandle> outputs() {
    return Map.copyOf(outputs);
  }

  @Override
  public String toString() {
    return "TaskHandle[" +
      "sessionId=" + sessionId + ", " +
      "configuration=" + configuration + ", " +
      "inputs=" + inputs + ", " +
      "outputs=" + outputs +  ']';
  }
}
