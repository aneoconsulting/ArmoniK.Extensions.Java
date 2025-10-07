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

import fr.aneo.armonik.api.grpc.v1.events.EventsGrpc;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import fr.aneo.armonik.client.internal.grpc.observers.BlobCompletionEventObserver;
import io.grpc.ManagedChannel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static fr.aneo.armonik.client.internal.grpc.mappers.EventMapper.createEventSubscriptionRequest;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Internal component responsible for watching blob completion events in the ArmoniK cluster.
 * <p>
 * This class establishes gRPC event streams to monitor blob state changes and coordinates
 * with {@link BlobCompletionListener} instances to deliver completion notifications.
 * It serves as the implementation backend for blob completion watching operations.
 * <p>
 * BlobCompletionEventWatcher is an internal implementation class and should not be used
 * directly by client applications. Use {@link BlobCompletionCoordinator} or
 * {@link SessionHandle#awaitOutputsProcessed()} for blob completion monitoring.
 *
 * @see BlobCompletionListener
 * @see BlobCompletionCoordinator
 * @see SessionHandle#awaitOutputsProcessed()
 */
final class BlobCompletionEventWatcher {

  private final EventsGrpc.EventsStub eventsStub;

  /**
   * Creates a new blob completion event watcher using the specified gRPC channel.
   * <p>
   * The watcher will use the provided channel to establish event streams with the
   * ArmoniK cluster for monitoring blob state changes.
   *
   * @param channel the gRPC channel for cluster communication
   * @throws NullPointerException if channel is null
   */
  BlobCompletionEventWatcher(ManagedChannel channel) {
    this.eventsStub = EventsGrpc.newStub(channel);
  }

  /**
   * Starts watching the specified blob handles for completion events.
   * <p>
   * This method establishes an event stream to monitor the provided blob handles
   * and invokes the listener when completion events occur. The returned completion
   * stage completes when all watched blobs reach terminal states (completed or failed).
   * <p>
   * The watching operation is asynchronous and non-blocking. The listener will be
   * invoked on gRPC event threads as blob state changes are received from the cluster.
   *
   * @param sessionId   the session identifier for the blobs to watch
   * @param blobHandles the list of blob handles to monitor for completion
   * @param listener    the listener to receive completion events
   * @return a completion stage that completes when all watched blobs are terminal
   * @throws NullPointerException if any parameter is null
   * @see BlobCompletionListener
   * @see BlobHandle
   */
  public CompletionStage<Void> watch(SessionId sessionId, List<BlobHandle> blobHandles, BlobCompletionListener listener) {
    requireNonNull(sessionId, "sessionId must not be null");
    requireNonNull(blobHandles, "blobHandles must not be null");
    requireNonNull(listener, "listener must not be null");

    CompletableFuture<Void> completion = new CompletableFuture<>();
    handlesById(blobHandles).thenAccept(handlesById -> eventsStub.getEvents(createEventSubscriptionRequest(sessionId, handlesById.keySet()), new BlobCompletionEventObserver(sessionId, handlesById, completion, listener)))
                            .exceptionally(ex -> {
                              completion.completeExceptionally(ex);
                              return null;
                            });
    return completion;
  }


  private static CompletionStage<Map<BlobId, BlobHandle>> handlesById(List<BlobHandle> handles) {
    return Futures.allOf(handles.stream()
                                .map(handle -> handle.deferredBlobInfo().thenApply(info -> Map.entry(info.id(), handle)))
                                .toList())
                  .thenApply(entry -> entry.stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }
}
