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

import io.grpc.ManagedChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

import static fr.aneo.armonik.client.internal.concurrent.Futures.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Basic batching utility for managing multiple blob completion watching operations.
 * <p>
 * This class provides a simple mechanism to coordinate multiple concurrent blob watching
 * operations through a single {@link BlobCompletionEventWatcher} instance. It allows
 * enqueueing multiple watch requests and waiting for their collective completion.
 *
 * <p>
 * <strong>Note:</strong> This is a basic implementation that will be fully reworked in
 * future versions. The current design is intended for simple use cases and may not be
 * suitable for high-throughput or complex batching scenarios.
 *
 * @see BlobCompletionEventWatcher
 * @see BlobCompletionListener
 */

public class BlobCompletionCoordinator {

  private final BlobCompletionEventWatcher watcher;
  private final BlobCompletionListener listener;

  private final Queue<CompletionStage<Void>> inFlightStages = new ConcurrentLinkedQueue<>();

  /**
   * Creates a new batcher that will use the specified watcher and listener for all operations.
   *
   * @param listener the listener to receive completion events; must not be {@code null}
   * @throws NullPointerException if either parameter is {@code null}
   */
  public BlobCompletionCoordinator(ManagedChannel channel, BlobCompletionListener listener) {
    this.watcher = new BlobCompletionEventWatcher(channel);
    this.listener = listener;
  }

  /**
   * Enqueues a new blob watching operation to be tracked by this batcher.
   *
   * @param sessionId the session context for the blobs to watch; must not be {@code null}
   * @param blobHandles   the blob handles to monitor; must not be {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   */
  public void enqueue(SessionId sessionId, List<BlobHandle> blobHandles) {
    inFlightStages.add(watcher.watch(sessionId, blobHandles, listener));
  }

  /**
   * Returns a completion stage that completes when all currently enqueued operations finish.
   * <p>
   * This method takes a snapshot of all in-flight operations at the time of invocation
   * and returns a stage that completes when all of them reach terminal states.
   * Operations enqueued after calling this method are not included in the returned stage.
   * </p>
   *
   * @return a completion stage that completes when all current operations finish,
   *         or a completed stage if no operations are in progress
   */
  public CompletionStage<Void> waitUntilFinished() {
    var snapshot = new ArrayList<>(inFlightStages);
    if (snapshot.isEmpty()) return completedFuture(null);

    return allOf(snapshot).thenApply(v -> null);
  }
}
