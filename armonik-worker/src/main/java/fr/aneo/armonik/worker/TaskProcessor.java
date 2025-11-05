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
package fr.aneo.armonik.worker;

/**
 * Functional interface for processing ArmoniK tasks within a Worker.
 * <p>
 * A {@code TaskProcessor} defines the core business logic executed by a Worker when it receives
 * a task from the ArmoniK Agent. Implementations receive a {@link TaskContext} that provides
 * access to task metadata, input data, and methods to produce outputs and submit subtasks.
 *
 * <h2>Task Processing Flow</h2>
 * <p>
 * When the ArmoniK Agent assigns a task to the Worker, the following occurs:
 * <ol>
 *   <li>The Worker receives a {@code ProcessRequest} from the Agent</li>
 *   <li>A {@link TaskContext} is created to manage task data and operations</li>
 *   <li>The {@link #processTask(TaskContext)} method is invoked with the context</li>
 *   <li>The implementation processes the task using the context's capabilities</li>
 *   <li>A {@link TaskOutcome} is returned indicating success or failure</li>
 *   <li>The Worker reports the outcome back to the Agent</li>
 * </ol>
 *
 * <h2>Implementation Guidelines</h2>
 * <p>
 * Implementations should:
 * <ul>
 *   <li><strong>Be idempotent</strong>: Tasks may be retried on failure, so processing the same
 *       task multiple times should produce the same result</li>
 *   <li><strong>Handle errors gracefully</strong>: Return {@link TaskOutcome.Error} for expected
 *       failures. Uncaught exceptions are converted to errors automatically</li>
 *   <li><strong>Use the TaskContext</strong>: Access inputs, write outputs, and submit subtasks
 *       through the provided context</li>
 *   <li><strong>Be stateless</strong>: Avoid storing state between task invocations</li>
 * </ul>
 *
 * <h2>Task Outcomes</h2>
 * <p>
 * Return values indicate task completion status:
 * </p>
 * <ul>
 *   <li>{@link TaskOutcome.Success}: Task completed successfully, all expected outputs were produced</li>
 *   <li>{@link TaskOutcome.Error}: Task failed with an error message. The task may be retried
 *       based on the {@code max_retries} configuration</li>
 * </ul>
 *
 * <h2>Exception Handling</h2>
 * <p>
 * If the implementation throws an uncaught exception:
 * </p>
 * <ul>
 *   <li>The exception is caught by the Worker</li>
 *   <li>The exception message is extracted and wrapped in a {@link TaskOutcome.Error}</li>
 *   <li>The task is marked as failed and may be retried</li>
 * </ul>
 *
 * @see TaskContext
 * @see TaskOutcome
 * @see ArmoniKWorker
 * @see <a href="https://armonik.readthedocs.io/en/latest/">ArmoniK Documentation</a>
 */
@FunctionalInterface
public interface TaskProcessor {

  /**
   * Processes a single task assigned by the ArmoniK Agent.
   * <p>
   * This method is called by the Worker for each task received from the Agent. The implementation
   * should use the provided {@code taskContext} to access task inputs, produce outputs, and
   * optionally submit subtasks.
   *
   * <h4>Processing Steps</h4>
   * <p>
   * Typical implementation steps:
   * <ol>
   *   <li>Perform the required computation or business logic</li>
   *   <li>Write outputs to expected output keys via the context</li>
   *   <li>Optionally submit new subtasks for dynamic graph creation</li>
   *   <li>Return {@link TaskOutcome.Success} or {@link TaskOutcome.Error}</li>
   * </ol>
   *
   * <h4>Idempotency</h4>
   * <p>
   * Tasks may be retried multiple times in case of failure or worker restart. Implementations
   * must be idempotent: processing the same task with the same inputs should always produce
   * the same outputs and side effects.
   *
   * <h4>Thread Safety</h4>
   * <p>
   * The Worker guarantees that only one task is processed at a time per Worker instance.
   * Implementations do not need to be thread-safe for concurrent task processing.
   *
   * @param taskContext provides access to task metadata, inputs, outputs, and submission capabilities;
   *                    never {@code null}
   * @return {@link TaskOutcome.Success} if the task completed successfully,
   * or {@link TaskOutcome.Error} if the task failed
   */
  TaskOutcome processTask(TaskContext taskContext);
}
