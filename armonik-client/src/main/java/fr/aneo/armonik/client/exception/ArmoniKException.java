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

package fr.aneo.armonik.client.exception;

/**
 * Base exception for all ArmoniK client operations.
 * <p>
 * This unchecked exception is thrown when an error occurs during communication
 * with the ArmoniK service or when processing ArmoniK-related operations.
 * It can wrap lower-level exceptions (such as gRPC or network errors) to provide
 * additional context about the failed operation.
 * </p>
 *
 * @see RuntimeException
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
   * This constructor is typically used to wrap lower-level exceptions
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
