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
 * Base runtime exception for all ArmoniK Worker-related errors.
 * <p>
 * {@code ArmoniKException} is thrown when operations within the Worker framework fail due to:
 * </p>
 * <ul>
 *   <li>I/O errors (file reading/writing failures)</li>
 *   <li>Invalid payload or configuration data</li>
 *   <li>Path validation failures (directory traversal attempts)</li>
 *   <li>File system errors (missing files, permission issues)</li>
 *   <li>Other infrastructure-level failures</li>
 * </ul>
 *
 * <h2>Exception Hierarchy</h2>
 * <p>
 * This exception extends {@link RuntimeException}, making it an unchecked exception.
 * Task processors can catch this exception to handle infrastructure failures gracefully
 * and convert them to appropriate {@link TaskOutcome} values.
 * </p>
 *
 * @see TaskProcessor
 * @see TaskOutcome
 * @see WorkerGrpc
 */
public class ArmoniKException extends RuntimeException {

  /**
   * Creates a new ArmoniK exception with the specified error message.
   *
   * @param message the detail message explaining the error; should not be {@code null}
   */
  public ArmoniKException(String message) {
    super(message);
  }

  /**
   * Creates a new ArmoniK exception with the specified error message and cause.
   * <p>
   * This constructor is typically used to wrap lower-level exceptions (e.g., {@link java.io.IOException})
   * with additional context about the ArmoniK operation that failed.
   * </p>
   *
   * @param message the detail message explaining the error; should not be {@code null}
   * @param cause the underlying cause of this exception; may be {@code null}
   */
  public ArmoniKException(String message, Throwable cause) {
    super(message, cause);
  }
}
