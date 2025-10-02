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
 * BlobCompletionEventWatcher is an internal implementation class and should not be used
 * directly by client applications. Use {@link BlobCompletionCoordinator} or
 * {@link SessionHandle#awaitOutputsProcessed()} for blob completion monitoring.
 *
 * @see BlobCompletionListener
 * @see BlobCompletionCoordinator
 * @see SessionHandle#awaitOutputsProcessed()
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
   * and invokes the listener when completion events occur. The returned completion
   * stage completes when all watched blobs reach terminal states (completed or failed).
   * <p>
   * The watching operation is asynchronous and non-blocking. The listener will be
   * invoked on gRPC event threads as blob state changes are received from the cluster.
   *
   * @param blobHandles the list of blob handles to monitor for completion
   * @return a completion stage that completes when all watched blobs are terminal
   * @throws NullPointerException if any parameter is null
   * @see BlobCompletionListener
   * @see BlobHandle
   */
  public CompletionStage<Void> watch(List<BlobHandle> blobHandles) {
    requireNonNull(blobHandles, "blobHandles must not be null");
    logger.atDebug()
          .addKeyValue("operation", "watch")
          .addKeyValue("sessionId", sessionId.asString())
          .addKeyValue("count", blobHandles.size())
          .log("Starting watch for blob handles");

    CompletableFuture<Void> completion = new CompletableFuture<>();
    handlesById(blobHandles).thenAccept(handlesById -> {
      logger.atTrace()
            .addKeyValue("operation", "subscribe")
            .addKeyValue("sessionId", sessionId.asString())
            .addKeyValue("ids", handlesById.size())
            .log("Subscribing to event stream");

      eventsStub.getEvents(
        createEventSubscriptionRequest(sessionId, handlesById.keySet()),
        new BlobCompletionEventObserver(sessionId, handlesById, completion, listener)
      );
    }).exceptionally(ex -> {
      logger.atError()
            .addKeyValue("operation", "watchError")
            .addKeyValue("sessionId", sessionId.asString())
            .addKeyValue("error", ex.getClass().getSimpleName())
            .setCause(ex)
            .log("Failed to set up event subscription");

      completion.completeExceptionally(ex);
      return null;
    });

    completion.whenComplete((ok, ex) -> {
      boolean failed = (ex != null);
      logger.atDebug()
            .addKeyValue("operation", "watchComplete")
            .addKeyValue("sessionId", sessionId.asString())
            .addKeyValue("status", failed ? "failed" : "ok")
            .log("Watch completed");
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
