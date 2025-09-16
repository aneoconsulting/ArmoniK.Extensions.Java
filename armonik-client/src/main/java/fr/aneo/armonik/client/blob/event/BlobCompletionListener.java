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
package fr.aneo.armonik.client.blob.event;

import fr.aneo.armonik.client.blob.BlobMetadata;

/**
 * Callback interface for receiving asynchronous notifications about blob completion events.
 * <p>
 * The {@code BlobCompletionListener} provides a reactive programming model for handling blob
 * state changes within the ArmoniK distributed computing platform. This interface is primarily
 * used to receive notifications when task outputs become available for download or when blob
 * operations fail.
 *
 * <h2>Event-Driven Task Processing</h2>
 * <p>
 * In ArmoniK's asynchronous execution model, tasks run on remote workers and produce outputs
 * at unpredictable times. Rather than polling for completion, applications can register a
 * {@code BlobCompletionListener} to receive immediate notifications when:
 * <ul>
 *   <li>Task outputs are successfully created and available for download</li>
 *   <li>Task execution fails, causing expected outputs to become unavailable</li>
 *   <li>Network or storage issues prevent blob operations from completing</li>
 * </ul>
 *
 *
 * <p>
 * Implementations of this interface must be thread-safe, as callback methods may be invoked
 * concurrently from multiple threads within the ArmoniK client's event processing system.
 * The callbacks are typically invoked on gRPC event loop threads or executor service threads,
 * not on the calling application's main thread.
 * </p>
 *
 * <h2>Exception Handling</h2>
 * <p>
 * Implementations should handle exceptions gracefully within callback methods. Uncaught
 * exceptions thrown from callback methods are logged but do not interrupt the event stream
 * or affect the processing of other events. However, they may prevent proper resource
 * cleanup or error reporting for the specific event that triggered the exception.
 * </p>
 *
 * <h2>Performance Considerations</h2>
 * <p>
 * Callback methods should complete quickly to avoid blocking the event processing pipeline.
 * For CPU-intensive or I/O-bound operations, consider offloading work to a separate thread
 * or executor service:
 * </p>
 * <pre>{@code
 * private final Executor processingExecutor = Executors.newCachedThreadPool();
 *
 * @Override
 * public void onBlobCompleted(Blob blob) {
 *     processingExecutor.execute(() -> {
 *         // Perform heavy processing off the event thread
 *         processLargeResult(blob.data());
 *     });
 * }
 * }</pre>
 *
 * @see BlobCompletionEventWatcher
 * @see Blob
 * @see fr.aneo.armonik.client.ArmoniKClientBuilder#withTaskOutputListener(BlobCompletionListener)
 * @see fr.aneo.armonik.client.blob.BlobHandle
 * @see fr.aneo.armonik.client.task.TaskHandle
 */
public interface BlobCompletionListener {

  void onSuccess(Blob blob);

  void onError(BlobError blobError);

  record Blob(BlobMetadata metadata, byte[] data) {
  }

  record BlobError(BlobMetadata metadata, Throwable cause) {
  }
}
