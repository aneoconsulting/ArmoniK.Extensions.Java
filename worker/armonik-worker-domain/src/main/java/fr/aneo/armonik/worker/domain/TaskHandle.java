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
package fr.aneo.armonik.worker.domain;

import fr.aneo.armonik.worker.domain.definition.task.TaskDefinition;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Handle representing a submitted task within the ArmoniK distributed computing platform.
 * <p>
 * A task handle provides access to task metadata, configuration, and associated blob
 * handles for inputs and outputs. Task handles are immutable references created when
 * tasks are submitted via {@link TaskContext#submitTask(TaskDefinition)}.
 * </p>
 *
 * <h2>Usage Context</h2>
 * <p>
 * <strong>Production:</strong> Task handles are created by {@link TaskContext} when
 * submitting subtasks during task execution. The handle represents a task that has been
 * successfully submitted to the ArmoniK cluster and is either queued, running, or completed.
 * </p>
 *
 * <h2>Task Inputs and Outputs</h2>
 * <p>
 * The handle provides access to blob handles for both inputs and outputs:
 * </p>
 * <ul>
 *   <li><strong>Inputs</strong>: Data provided to the task for processing. These may be
 *       new blobs created during submission, or references to existing blobs (e.g., parent
 *       task inputs reused by the subtask).</li>
 *   <li><strong>Outputs</strong>: Expected results that the task will produce. These may be
 *       new output blobs, or delegated outputs where the subtask produces results on behalf
 *       of its parent task.</li>
 * </ul>
 *
 * <h2>Asynchronous Task Information</h2>
 * <p>
 * The task's metadata ({@link TaskInfo}) is provided asynchronously through
 * {@link #deferredTaskInfo()}. This completion stage completes when the ArmoniK
 * cluster acknowledges the task submission and assigns the task identifier.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * {@code TaskHandle} instances are immutable and thread-safe. Multiple threads
 * can safely access the same handle concurrently.
 * </p>
 *
 * @see TaskInfo
 * @see TaskConfiguration
 * @see BlobHandle
 * @see TaskContext#submitTask(TaskDefinition)
 */
public final class TaskHandle {
  private final SessionId sessionId;
  private final TaskConfiguration configuration;
  private final CompletionStage<TaskInfo> deferredTaskInfo;
  private final Map<String, BlobHandle> inputs;
  private final Map<String, BlobHandle> outputs;

  /**
   * Creates a task handle with the specified properties.
   * <p>
   * <strong>Usage Context:</strong> In production environments, task handles are
   * typically created by {@link TaskContext} when submitting subtasks. This public
   * constructor is primarily provided for testing purposes when implementing custom
   * task processors.
   * </p>
   *
   * @param sessionId        the session in which the task was submitted; must not be {@code null}
   * @param configuration    the task configuration specifying execution parameters;
   *                         may be {@code null} to use session defaults
   * @param deferredTaskInfo completion stage that will provide task metadata when the
   *                         cluster acknowledges task submission; must not be {@code null}
   * @param inputs           map of input blob handles, keyed by logical name; must not be
   *                         {@code null} but may be empty
   * @param outputs          map of output blob handles, keyed by logical name; must not be
   *                         {@code null} but may be empty
   * @throws NullPointerException if any required parameter is {@code null}
   * @see TaskContext#submitTask(TaskDefinition)
   */
  public TaskHandle(
    SessionId sessionId,
    TaskConfiguration configuration,
    CompletionStage<TaskInfo> deferredTaskInfo,
    Map<String, BlobHandle> inputs,
    Map<String, BlobHandle> outputs) {

    this.sessionId = Objects.requireNonNull(sessionId, "sessionId cannot be null");
    this.configuration = configuration;  // 👈 Can be null - uses session defaults
    this.deferredTaskInfo = Objects.requireNonNull(deferredTaskInfo, "deferredTaskInfo cannot be null");
    this.inputs = Objects.requireNonNull(inputs, "inputs cannot be null");
    this.outputs = Objects.requireNonNull(outputs, "outputs cannot be null");
  }

  /**
   * Returns the identifier of the session in which this task was submitted.
   * <p>
   * All tasks belong to a session context that defines their execution environment
   * and resource allocation settings.
   * </p>
   *
   * @return the session identifier; never {@code null}
   * @see SessionId
   */
  public SessionId sessionId() {
    return sessionId;
  }

  /**
   * Returns the configuration applied to this task, if specified.
   * <p>
   * The task configuration defines execution parameters such as priority, maximum duration,
   * maximum retries, partition selection, and other task-specific settings. If {@code null},
   * the task uses the default configuration from its session.
   * </p>
   *
   * @return the task configuration, or {@code null} if using session defaults
   * @see TaskConfiguration
   */
  public TaskConfiguration taskConfiguration() {
    return configuration;
  }

  /**
   * Returns a completion stage that provides immutable metadata about this task.
   * <p>
   * The task information becomes available after the ArmoniK cluster acknowledges
   * the task submission and assigns the task identifier. The returned completion stage
   * completes normally with {@link TaskInfo} when metadata is available, or completes
   * exceptionally if task submission fails.
   * </p>
   * <p>
   * This method provides non-blocking access to task metadata, including the
   * task's unique identifier assigned by the cluster.
   * </p>
   *
   * <h4>Usage Example</h4>
   * <pre>{@code
   * TaskHandle handle = taskContext.submitTask(taskDefinition);
   *
   * // Non-blocking access to task ID
   * handle.deferredTaskInfo()
   *       .thenAccept(info -> {
   *         TaskId id = info.id();
   *         System.out.println("Task submitted with ID: " + id);
   *       });
   * }</pre>
   *
   * @return a completion stage that completes with task metadata; never {@code null}
   * @see TaskInfo
   */
  public CompletionStage<TaskInfo> deferredTaskInfo() {
    return deferredTaskInfo;
  }

  /**
   * Returns an immutable map of input blob handles used by this task.
   * <p>
   * Input blobs contain the data required for task execution. Each blob is identified
   * by a logical name (the map key) that corresponds to the input names specified in the
   * task definition. The map may be empty if the task has no inputs.
   * </p>
   * <p>
   * The returned map is a defensive copy. Modifications to the returned map do not
   * affect the task handle's internal state.
   * </p>
   *
   * @return an immutable map of input blob handles, keyed by logical name; never {@code null},
   *         may be empty if the task has no inputs
   * @see BlobHandle
   */
  public Map<String, BlobHandle> inputs() {
    return Map.copyOf(inputs);
  }

  /**
   * Returns an immutable map of expected output blob handles for this task.
   * <p>
   * Output blobs represent the results that will be produced by task execution.
   * Each blob is identified by a logical name (the map key) that corresponds to the
   * output names declared in the task definition. The map may be empty if the task
   * has no outputs.
   * </p>
   * <p>
   * Output blob handles can be used to monitor result availability or download result
   * data once the task completes and produces the expected outputs.
   * </p>
   * <p>
   * The returned map is a defensive copy. Modifications to the returned map do not
   * affect the task handle's internal state.
   * </p>
   *
   * @return an immutable map of output blob handles, keyed by logical name; never {@code null},
   *         may be empty if the task has no outputs
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
      "inputs=" + inputs.keySet() + ", " +
      "outputs=" + outputs.keySet() + ']';
  }
}
