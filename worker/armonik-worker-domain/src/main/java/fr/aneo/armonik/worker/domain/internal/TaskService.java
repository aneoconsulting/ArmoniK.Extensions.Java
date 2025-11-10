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
package fr.aneo.armonik.worker.domain.internal;

import fr.aneo.armonik.worker.domain.BlobId;
import fr.aneo.armonik.worker.domain.TaskConfiguration;
import fr.aneo.armonik.worker.domain.TaskContext;
import fr.aneo.armonik.worker.domain.TaskInfo;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
/**
 * Service responsible for submitting tasks to ArmoniK.
 * <p>
 * This service handles task creation and submission within a session. Tasks submitted
 * through this service are subtasks of the currently executing task and will be processed
 * according to their configuration and dependencies.
 * </p>
 *
 * <h2>Task Structure</h2>
 * <p>
 * Each task submitted consists of:
 * </p>
 * <ul>
 *   <li><strong>Payload</strong>: A special input blob containing the mapping between logical
 *       names and blob IDs for inputs and outputs</li>
 *   <li><strong>Data Dependencies</strong>: Input blobs that must be available before the task
 *       can execute</li>
 *   <li><strong>Expected Outputs</strong>: Output blobs that the task is expected to produce</li>
 *   <li><strong>Task Configuration</strong>: Execution parameters (priority, retries, partition, etc.)</li>
 * </ul>
 *
 * @see TaskConfiguration
 * @see BlobService
 * @see Payload
 * @see TaskContext
 */
public interface TaskService {
  /**
   * Submits a task for execution to ArmoniK.
   * <p>
   * This method creates a task with the specified dependencies, outputs, payload, and configuration,
   * then submits it to ArmoniK for scheduling and execution. The task will not execute until all
   * its data dependencies are available.
   *
   * <h4>Validation Rules</h4>
   * <p>
   * All parameters must satisfy:
   * </p>
   * <ul>
   *   <li>{@code inputIds} must not be {@code null} or empty</li>
   *   <li>{@code outputIds} must not be {@code null} or empty</li>
   *   <li>{@code payloadId} must not be {@code null}</li>
   * </ul>
   *
   * @param inputIds          collection of blob IDs for task inputs (data dependencies);
   *                          must not be {@code null} or empty
   * @param outputIds         collection of blob IDs for expected task outputs;
   *                          must not be {@code null} or empty
   * @param payloadId         blob ID of the payload containing the input/output mapping;
   *                          must not be {@code null}
   * @param taskConfiguration configuration for this task;
   *                          must not be {@code null}
   * @return a completion stage that completes with task information when the Agent acknowledges
   * the submission; never {@code null}
   * @throws NullPointerException     if any parameter is {@code null}
   * @throws IllegalArgumentException if {@code inputIds} or {@code outputIds} is empty
   */
  CompletionStage<TaskInfo> submitTask(Collection<BlobId> inputIds, Collection<BlobId> outputIds, BlobId payloadId, TaskConfiguration taskConfiguration);
}
