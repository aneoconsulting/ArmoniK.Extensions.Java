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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * The watcher manages event subscriptions per session and provides mechanisms to track
 * both successful completions and leftover blob handles that weren't observed during
 * the watch period (useful for retry scenarios).
 * <p>
 * BlobCompletionEventWatcher is an internal implementation class and should not be used
 * directly by client applications. Use {@link BlobCompletionCoordinator} or
 * {@link SessionHandle#awaitOutputsProcessed()} for blob completion monitoring.
 *
 * @see BlobCompletionListener
 * @see BlobCompletionCoordinator
 * @see SessionHandle#awaitOutputsProcessed()
 * @see WatchTicket
 */
final class BlobCompletionEventWatcher {

  private static final Logger logger = LoggerFactory.getLogger(BlobCompletionEventWatcher.class);

  private final EventsGrpc.EventsStub eventsStub;
  private final BlobCompletionListener listener;
  private final SessionId sessionId;

  /**
   * Creates a new blob completion event watcher using the specified gRPC channel.
   * <p>
   * The watcher will use the provided channel to establish event streams with the
   * ArmoniK cluster for monitoring blob state changes.
   *
   * @param sessionId the identifier of the session this watcher serves
   * @param channel   the gRPC channel for cluster communication
   * @param listener  the listener to receive completion events
   * @throws NullPointerException if channel is null
   */
  BlobCompletionEventWatcher(SessionId sessionId, ManagedChannel channel, BlobCompletionListener listener) {
    requireNonNull(sessionId, "sessionId must not be null");
    requireNonNull(channel, "channel must not be null");
    requireNonNull(listener, "listener must not be null");

    this.eventsStub = EventsGrpc.newStub(channel);
    this.sessionId = sessionId;
    this.listener = listener;
  }

  /**
   * Starts watching the specified blob handles for completion events.
   * <p>
   * This method establishes an event stream to monitor the provided blob handles
   * and invokes the listener when completion events occur. The returned watch ticket
   * provides access to both the completion status and any leftover blob handles
   * that weren't observed during the watch period.
   * <p>
   * The watching operation is asynchronous and non-blocking. The listener will be
   * invoked on gRPC event threads as blob state changes are received from the cluster.
   * <p>
   * <strong>Leftover Handling:</strong> If the event stream closes before all blobs
   * reach terminal states, the unobserved blob handles are made available through
   * {@link WatchTicket#leftoversAfterCompletion()} for potential resubmission.
   *
   * @param blobHandles the list of blob handles to monitor for completion
   * @return a watch ticket providing access to completion status and leftover handles
   * @throws NullPointerException if blobHandles is null
   * @see BlobCompletionListener
   * @see BlobHandle
   * @see WatchTicket
   */
  public WatchTicket watch(List<BlobHandle> blobHandles) {
    requireNonNull(blobHandles, "blobHandles must not be null");

    var ticket = new WatchTicket();
    handlesById(blobHandles).thenAccept(handlesById -> eventsStub.getEvents(
      createEventSubscriptionRequest(sessionId, handlesById.keySet()),
      new BlobCompletionEventObserver(sessionId, handlesById, ticket.completion, listener, ticket::setLeftoverHandles)
    )).exceptionally(ex -> {
      logger.error("Failed to set up event subscription. SessionId: {}", sessionId, ex);

      ticket.completion.completeExceptionally(ex);
      return null;
    });

    return ticket;
  }


  private static CompletionStage<Map<BlobId, BlobHandle>> handlesById(List<BlobHandle> handles) {
    return Futures.allOf(handles.stream()
                                .map(handle -> handle.deferredBlobInfo().thenApply(info -> Map.entry(info.id(), handle)))
                                .toList())
                  .thenApply(entry -> entry.stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  /**
   * Internal ticket representing a blob watching operation.
   * <p>
   * A watch ticket provides access to the completion status of a blob watching
   * operation and tracks any leftover blob handles that weren't observed during
   * the watch period. This is useful for implementing retry logic or cleanup operations.
   * <p>
   * The ticket follows a completion model where the main completion future indicates
   * when the watching operation has finished (either successfully or due to stream
   * closure), while the leftover future provides access to unobserved blob handles.
   */
  static class WatchTicket {
    private final CompletableFuture<Void> completion;
    private final CompletableFuture<List<BlobHandle>> leftovers;

    WatchTicket() {
      this.completion = new CompletableFuture<>();
      this.leftovers = new CompletableFuture<>();
    }

    WatchTicket(CompletableFuture<Void> completion, List<BlobHandle> leftoverHandles) {
      this.completion = (completion != null ? completion : new CompletableFuture<>());
      this.leftovers = new CompletableFuture<>();
      this.leftovers.complete(leftoverHandles == null ? List.of() : List.copyOf(leftoverHandles));
    }

    /**
     * Returns a completion stage that completes when the watch operation finishes.
     * <p>
     * This stage completes normally when all watched blobs reach terminal states
     * or when the event stream is closed by the server. It may complete exceptionally
     * if the initial setup fails.
     *
     * @return a completion stage for the watch operation
     */
    CompletionStage<Void> completion() { return completion; }

    private void setLeftoverHandles(List<BlobHandle> handles) {
      var snapshot = (handles == null ? List.<BlobHandle>of() : List.copyOf(handles));
      leftovers.complete(snapshot);
    }

    /**
     * Returns a completion stage that provides leftover blob handles after watch completion.
     * <p>
     * This method provides access to blob handles that were being watched but never
     * observed as completed or aborted before the event stream closed. These handles
     * may need to be resubmitted to a new watch operation or handled through other
     * retry mechanisms.
     * <p>
     * The returned stage is guaranteed to complete after {@link #completion()} completes.
     * If there are no leftover handles, the stage completes with an empty immutable list.
     *
     * @return a completion stage yielding leftover blob handles after watch completion
     */
    CompletionStage<List<BlobHandle>> leftoversAfterCompletion() {
      return completion.handle((v, ex) -> null).thenCompose(ignored -> leftovers);
    }
  }
}
