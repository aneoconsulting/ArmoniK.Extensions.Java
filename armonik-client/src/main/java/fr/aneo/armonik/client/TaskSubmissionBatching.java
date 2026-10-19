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
package fr.aneo.armonik.client;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Controls how {@link SessionHandle#submitTask(fr.aneo.armonik.client.definition.TaskDefinition)}
 * groups tasks into batched submission calls.
 * <p>
 * Submitted tasks are buffered per session. The buffer is sent to the cluster when it holds
 * {@code batchSize} tasks, or {@code window} after the first buffered task, whichever comes first.
 * {@link SessionHandle#submitTasks(java.util.List)}, {@link SessionHandle#flushSubmissions()},
 * {@link SessionHandle#awaitOutputsProcessed()} and {@link SessionHandle#close()} send the buffer
 * immediately.
 * <p>
 * {@link #DISABLED} sends every task on its own as soon as it is submitted.
 *
 * <pre>{@code
 * var config = ArmoniKConfig.builder()
 *                           .endpoint("http://localhost:5001")
 *                           .withTaskSubmissionBatching(new TaskSubmissionBatching(500, Duration.ofMillis(50)))
 *                           .build();
 * }</pre>
 *
 * @param batchSize maximum number of tasks sent together; must be at least 1
 * @param window    maximum time a task waits in the buffer; must not be negative. {@link Duration#ZERO}
 *                  sends the buffer at each submission.
 * @see ArmoniKConfig.Builder#withTaskSubmissionBatching(TaskSubmissionBatching)
 */
public record TaskSubmissionBatching(int batchSize, Duration window) {

  /**
   * Default batching: up to 1000 tasks, sent at most 100 ms after the first buffered task.
   */
  public static final TaskSubmissionBatching DEFAULT = new TaskSubmissionBatching(1000, Duration.ofMillis(100));

  /**
   * No batching: each task is sent as soon as it is submitted.
   */
  public static final TaskSubmissionBatching DISABLED = new TaskSubmissionBatching(1, Duration.ZERO);

  public TaskSubmissionBatching {
    if (batchSize < 1) throw new IllegalArgumentException("batchSize must be at least 1, actual: " + batchSize);
    requireNonNull(window, "window must not be null");
    if (window.isNegative()) throw new IllegalArgumentException("window must not be negative, actual: " + window);
  }
}
