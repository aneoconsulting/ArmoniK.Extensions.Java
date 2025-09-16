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

import fr.aneo.armonik.client.blob.BlobHandle;
import fr.aneo.armonik.client.session.SessionHandle;

import java.util.List;
import java.util.concurrent.CompletionStage;


/**
 * Service for monitoring blob completion events within an ArmoniK session.
 * <p>
 * The {@code BlobCompletionEventWatcher} provides asynchronous notification capabilities for tracking
 * when blobs (particularly task outputs) reach terminal states in the ArmoniK cluster. This service
 * establishes a server-side streaming connection to the ArmoniK Control Plane to receive real-time
 * events about blob state changes.
 *
 * <h2>Event-Driven Workflow</h2>
 * <p>
 * In the ArmoniK distributed computing model, tasks produce outputs that become available asynchronously
 * as workers complete their execution. Rather than polling for completion status, this watcher enables
 * reactive programming patterns by streaming completion events directly from the server.
 *
 * <h2>Blob States and Events</h2>
 * <p>
 * Blobs can transition through various states during task execution:
 * <ul>
 *   <li><strong>PENDING:</strong> Blob declared but not yet produced</li>
 *   <li><strong>COMPLETED:</strong> Blob successfully created and available for download</li>
 *   <li><strong>ABORTED:</strong> Blob creation failed due to task failure or cancellation</li>
 * </ul>
 *
 * <h2>Usage Patterns</h2>
 * <pre>{@code
 * // Watch for task output completion
 * BlobCompletionEventWatcher watcher = services.blobCompletionEventWatcher();
 *
 * List<BlobHandle> outputHandles = Arrays.asList(taskHandle.outputs().values());
 *
 * CompletionStage<Void> watchResult = watcher.watch(
 *     sessionHandle,
 *     outputHandles,
 *     new BlobCompletionListener() {
 *         @Override
 *         public void onBlobCompleted(Blob blob) {
 *             // Download and process completed output
 *             System.out.println("Output ready: " + blob.metadata.id());
 *         }
 *
 *         @Override
 *         public void onBlobFailed(Blob blob, Throwable error) {
 *             // Handle failed output
 *             System.err.println("Output failed: " + blob.metadata.id());
 *         }
 *     }
 * );
 *
 * // The future completes when all watched blobs are terminal
 * watchResult.thenRun(() -> System.out.println("All outputs completed or failed"));
 * }</pre>
 *
 * @see BlobCompletionListener
 * @see fr.aneo.armonik.client.blob.BlobHandle
 * @see fr.aneo.armonik.client.session.SessionHandle
 * @see fr.aneo.armonik.client.Services#blobCompletionEventWatcher()
 */
public interface BlobCompletionEventWatcher {

  /**
   * Starts watching the specified blob handles for completion events within the given session.
   * <p>
   * This method establishes a server-side streaming connection to the ArmoniK Control Plane
   * and monitors the provided blob handles for state transitions. The {@link BlobCompletionListener}
   * receives callbacks as blobs complete successfully or fail.
   *
   * <p>
   * The method returns immediately with a {@link CompletionStage} that represents the overall
   * watching operation. This stage completes successfully when all watched blobs reach terminal
   * states (either COMPLETED or ABORTED), or completes exceptionally if the streaming connection
   * fails or is interrupted.
   *
   * <p>
   * Events are delivered in the order they occur on the server, but completion notifications
   * for different blobs may arrive in any order depending on task execution timing. The listener
   * callbacks are invoked asynchronously and should handle concurrent access appropriately.
   *
   * @param sessionHandle the session context that owns the blobs being watched; must not be {@code null}
   * @param blobHandles   list of blob handles to monitor for completion; must not be {@code null},
   *                      but may be empty (in which case the operation completes immediately)
   * @param listener      callback interface for receiving completion events; must not be {@code null}
   * @return a {@link CompletionStage} that completes when all watched blobs reach terminal states,
   *         or completes exceptionally if the watching operation fails
   * @throws NullPointerException if any parameter is {@code null}
   * @throws IllegalArgumentException if any blob handle belongs to a different session than {@code sessionHandle}
   * @see BlobCompletionListener#onSuccess(BlobCompletionListener.Blob)
   * @see BlobCompletionListener#onError(BlobCompletionListener.BlobError)
   */

  CompletionStage<Void> watch(SessionHandle sessionHandle,
                              List<BlobHandle> blobHandles,
                              BlobCompletionListener listener);
}
