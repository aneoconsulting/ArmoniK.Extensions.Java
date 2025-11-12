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
 * Models the outcome of processing a single ArmoniK task.
 * <p>
 * Instances of {@code TaskOutcome} are produced by {@link TaskProcessor#processTask(TaskContext)}
 * and consumed by {@link WorkerGrpc} to build the gRPC {@code Output} message returned to the Agent.
 * This type is deliberately minimal and immutable to keep the processing pipeline simple and
 * predictable.
 * </p>
 *
 * <h2>Protocol Mapping</h2>
 * <ul>
 *   <li>{@link Success} &rarr; gRPC {@code Output.ok = Empty}</li>
 *   <li>{@link Error} &rarr; gRPC {@code Output.error.details = message}</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <ul>
 *   <li>Return {@link #SUCCESS} when the task completed and all expected
 *       outputs were produced.</li>
 *   <li>Return {@link #error(String)} or {@link #error(Throwable)} to signal a managed failure with a
 *       human‑readable message. Uncaught exceptions are converted to {@link Error} by
 *       {@link WorkerGrpc}.</li>
 * </ul>
 *
 * @see TaskProcessor
 * @see WorkerGrpc
 * @see ArmoniKWorker
 */
public sealed interface TaskOutcome {

  /**
   * Singleton instance representing a successful task outcome.
   */
  TaskOutcome.Success SUCCESS = new Success();

  /**
   * Convenience factory to create an {@link Error} from a message. If {@code message} is {@code null},
   * a generic placeholder is used.
   *
   * @param message human-readable error description; may be {@code null}
   * @return an error outcome wrapping the message
   */
  static Error error(String message) {
    return new Error(message == null ? "Unknown error" : message);
  }

  /**
   * Convenience factory to create an {@link Error} from a {@link Throwable}. If the throwable has no
   * message, {@code throwable.toString()} is used. If {@code throwable} is {@code null}, a generic
   * placeholder is used.
   *
   * @param throwable the cause of the failure; may be {@code null}
   * @return an error outcome wrapping a message extracted from the throwable
   */
  static Error error(Throwable throwable) {
    String msg = (throwable == null)
      ? "Unknown error"
      : (throwable.getMessage() != null ? throwable.getMessage() : throwable.toString());
    return new Error(msg);
  }

  /**
   * Successful task completion.
   * <p>
   * Maps to gRPC {@code Output.ok = Empty}. No additional data is carried here; outputs must be
   * written via {@link TaskContext} prior to returning this outcome.
   * </p>
   */
  record Success() implements TaskOutcome {}

  /**
   * Task failure carrying a human‑readable message.
   * <p>
   * Maps to gRPC {@code Output.error.details = message}. Prefer short, actionable messages that the
   * control plane and client applications can log and expose.
   * </p>
   *
   * @param message non-null error details
   */
  record Error(String message) implements TaskOutcome {
    public Error {
      if (message == null) {
        message = "Unknown error";
      }
    }
  }
}
