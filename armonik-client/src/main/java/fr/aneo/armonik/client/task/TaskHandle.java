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

import fr.aneo.armonik.client.blob.BlobHandle;
import fr.aneo.armonik.client.session.SessionHandle;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Handle representing a submitted task.
 * <p>
 * A {@code TaskHandle} provides access to the {@link SessionHandle} in which the task runs,
 * the {@link TaskConfiguration} that applies to it, the asynchronous {@link #metadata()}
 * with task identifiers, and references to input/output blobs involved in the submission.
 * </p>
 *
 * <p>
 * The handle itself does not perform blocking operations. The {@linkplain #metadata() metadata future}
 * completes when the backend acknowledges and returns task information, and may complete exceptionally
 * if the submission fails.
 * </p>
 *
 * @see TaskMetadata
 * @see TaskConfiguration
 * @see BlobHandle
 * @see SessionHandle
 */

public final class TaskHandle {
  private final SessionHandle sessionHandle;
  private final TaskConfiguration taskConfiguration;
  private final CompletionStage<TaskMetadata> metadata;
  private final Map<String, BlobHandle> inputs;
  private final Map<String, BlobHandle> outputs;
  private final BlobHandle payLoad;

  /**
   * Creates a new task handle.
   *
   * @param sessionHandle           the session in which the task is submitted
   * @param taskConfiguration the configuration applied to the task (may be {@code null} if defaulted)
   * @param metadata          future that completes with task metadata
   * @param inputs            collection of input blob handles associated with the task
   * @param outputs           collection of expected output blob handles associated with the task
   * @param payLoad           the primary payload blob handle, if applicable
   */
  TaskHandle(
    SessionHandle sessionHandle,
    TaskConfiguration taskConfiguration,
    CompletionStage<TaskMetadata> metadata,
    Map<String, BlobHandle> inputs,
    Map<String, BlobHandle> outputs,
    BlobHandle payLoad
  ) {
    this.sessionHandle = sessionHandle;
    this.taskConfiguration = taskConfiguration;
    this.metadata = metadata;
    this.inputs = inputs;
    this.outputs = outputs;
    this.payLoad = payLoad;
  }

  /**
   * Returns the sessionHandle in which this task was submitted.
   *
   * @return the sessionHandle
   */
  public SessionHandle sessionHandle() {
    return sessionHandle;
  }

  /**
   * Returns the configuration applied to this task.
   *
   * @return the task configuration, or {@code null} if defaulted
   */
  public TaskConfiguration taskConfiguration() {
    return taskConfiguration;
  }

  /**
   * Returns a future that completes with immutable metadata of this task.
   * <p>
   * The returned future may complete exceptionally if task submission or retrieval fails.
   * </p>
   *
   * @return a future of task metadata
   */
  public CompletionStage<TaskMetadata> metadata() {
    return metadata;
  }

  /**
   * Returns the collection of input blob handles used for this task.
   *
   * @return the inputs collection
   */
  public Map<String, BlobHandle> inputs() {
    return Map.copyOf(inputs);
  }


  public Map<String, BlobHandle> outputs() {
    return Map.copyOf(outputs);
  }

  /**
   * Returns the payload blob handle for this task.
   *
   * @return the payload handle
   */

  public BlobHandle payLoad() {
    return payLoad;
  }

  @Override
  public String toString() {
    return "TaskHandle[" +
      "sessionHandle=" + sessionHandle + ", " +
      "configuration=" + taskConfiguration + ", " +
      "inputs=" + inputs + ", " +
      "outputs=" + outputs + ", " +
      "payLoad=" + payLoad + ']';
  }
}
