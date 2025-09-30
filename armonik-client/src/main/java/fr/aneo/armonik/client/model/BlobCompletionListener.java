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

import fr.aneo.armonik.client.definition.SessionDefinition;

/**
 * Callback interface for receiving asynchronous notifications about blob completion events.
 * <p>
 * The {@code BlobCompletionListener} provides a reactive programming model for handling blob
 * state changes within the ArmoniK distributed computing platform. This interface is primarily
 * used to receive notifications when task outputs become available or when blob operations fail.
 * <p>
 * The listener is registered when defining a session, allowing applications to specify how
 * they want to handle completion events for all task outputs within that session context.
 * <p>
 * In ArmoniK's asynchronous execution model, tasks run on remote workers and produce outputs
 * at unpredictable times. Rather than polling for completion, applications can register a
 * {@code BlobCompletionListener} to receive immediate notifications when outputs are ready.
 * <p>
 * Implementations of this interface must be thread-safe, as callback methods may be invoked
 * concurrently from multiple threads within the ArmoniK client's event processing system.
 *
 * @see BlobCompletionEventWatcher
 * @see BlobCompletionCoordinator
 * @see SessionHandle#awaitOutputsProcessed()
 * @see SessionDefinition
 */
public interface BlobCompletionListener {

  /**
   * Called when a blob has been successfully completed and is available for access.
   * <p>
   * This method is invoked when a task successfully produces an output blob or when
   * a blob operation completes successfully. The provided blob contains both metadata
   * and the actual data content.
   * <p>
   * Implementations should complete quickly to avoid blocking the event processing
   * pipeline. For CPU-intensive operations, consider offloading work to a separate
   * thread or executor service.
   *
   * @param blob the completed blob containing metadata and data content
   * @see Blob
   */
  void onSuccess(Blob blob);

  /**
   * Called when a blob operation fails or encounters an error.
   * <p>
   * This method is invoked when task execution fails, blob operations encounter
   * network issues, or other errors prevent blob completion. The error contains
   * both the blob metadata and the underlying cause of the failure.
   * <p>
   * Implementations should handle errors gracefully and should not throw exceptions
   * from this method, as uncaught exceptions may disrupt the event processing system.
   *
   * @param blobError the error information including blob metadata and failure cause
   * @see BlobError
   */
  void onError(BlobError blobError);

  /**
   * Record containing a completed blob's metadata and data content.
   * <p>
   * This record is provided to {@link #onSuccess(Blob)} when a blob operation
   * completes successfully.
   *
   * @param blobInfo the blob metadata including identifier
   * @param data the blob's data content as a byte array
   * @see BlobInfo
   */
  record Blob(BlobInfo blobInfo, byte[] data) {
  }

  /**
   * Record containing error information for a failed blob operation.
   * <p>
   * This record is provided to {@link #onError(BlobError)} when a blob operation
   * fails or encounters an error condition.
   *
   * @param blobInfo the blob metadata including identifier
   * @param cause the underlying exception or error that caused the failure
   * @see BlobInfo
   */
  record BlobError(BlobInfo blobInfo, Throwable cause) {
  }
}
