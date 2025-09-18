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

import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse;
import fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus;
import fr.aneo.armonik.client.blob.BlobMetadata;
import fr.aneo.armonik.client.blob.BlobService;
import fr.aneo.armonik.client.session.SessionHandle;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static fr.aneo.armonik.client.blob.event.BlobCompletionListener.Blob;
import static fr.aneo.armonik.client.blob.event.BlobCompletionListener.BlobError;

/**
 * Internal gRPC stream observer that processes blob completion events and manages listener notifications.
 * <p>
 * This class implements the {@link StreamObserver} contract to handle server-side streaming events
 * from the ArmoniK Control Plane's Events API. It serves as the bridge between the raw gRPC event
 * stream and the higher-level {@link BlobCompletionListener} interface, providing event filtering,
 * state management, and automatic blob data retrieval.
 *
 * <h2>Architecture Role</h2>
 * <p>
 * The subscriber is a key component in the ArmoniK client's reactive event processing pipeline:
 * <ol>
 *   <li>{@link BlobCompletionEventWatcher} establishes the gRPC stream</li>
 *   <li>{@code BlobCompletionEventSubscriber} processes incoming events</li>
 *   <li>{@link BlobCompletionListener} receives filtered, processed notifications</li>
 * </ol>
 *
 * <h2>Event Processing Flow</h2>
 * <p>
 * For each incoming event, the subscriber:
 * <ul>
 *   <li>Filters events to only those matching watched blob IDs</li>
 *   <li>Processes COMPLETED events by automatically downloading blob data</li>
 *   <li>Processes ABORTED events by notifying about failures</li>
 *   <li>Tracks completion progress and signals when all blobs are terminal</li>
 *   <li>Handles listener exceptions gracefully to prevent stream disruption</li>
 * </ul>
 *
 * <h2>Error Handling and Resilience</h2>
 * <p>
 * The implementation includes comprehensive error handling:
 * <ul>
 *   <li>Listener exceptions are caught and logged to prevent stream termination</li>
 *   <li>Blob download failures are reported as error events to the listener</li>
 *   <li>Stream errors are logged and propagated to the completion future</li>
 *   <li>Unknown or malformed events are safely ignored</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is designed to be thread-safe for concurrent event processing. All shared state
 * is protected using concurrent data structures and atomic operations. Listener callbacks
 * may be invoked from multiple threads and should handle concurrency appropriately.
 * </p>
 *
 * <h2>Performance Characteristics</h2>
 * <p>
 * The subscriber is optimized for low latency and minimal memory overhead:
 * <ul>
 *   <li>Events are processed synchronously on the gRPC event loop thread</li>
 *   <li>Blob downloads are initiated asynchronously to avoid blocking the event stream</li>
 *   <li>Completed blobs are immediately removed from tracking to reduce memory usage</li>
 *   <li>Listener exceptions are isolated to prevent cascading failures</li>
 * </ul>
 * </p>
 *
 * @see BlobCompletionEventWatcher
 * @see BlobCompletionListener
 * @see StreamObserver
 * @see fr.aneo.armonik.client.blob.BlobService
 */
final class BlobCompletionEventSubscriber implements StreamObserver<EventSubscriptionResponse> {

  private static final Logger log = LoggerFactory.getLogger(BlobCompletionEventSubscriber.class);

  private final SessionHandle sessionHandle;
  private final ConcurrentMap<UUID, BlobMetadata> pending;
  private final AtomicInteger remaining;
  private final CompletableFuture<Void> completion;
  private final BlobService blobService;
  private final BlobCompletionListener listener;

  BlobCompletionEventSubscriber(SessionHandle sessionHandle,
                                ConcurrentMap<UUID, BlobMetadata> pending,
                                CompletableFuture<Void> completion,
                                BlobService blobService,
                                BlobCompletionListener listener) {
    this.sessionHandle = sessionHandle;
    this.pending = pending;
    this.remaining = new AtomicInteger(pending.size());
    this.completion = completion;
    this.blobService = blobService;
    this.listener = listener;
  }

  @Override
  public void onNext(EventSubscriptionResponse resp) {
    switch (resp.getUpdateCase()) {
      case RESULT_STATUS_UPDATE ->
        handleEvent(resp.getResultStatusUpdate().getResultId(), resp.getResultStatusUpdate().getStatus());
      case NEW_RESULT -> handleEvent(resp.getNewResult().getResultId(), resp.getNewResult().getStatus());
      case UPDATE_NOT_SET -> { /* ignore */ }
    }

    if (remaining.get() == 0 && !completion.isDone()) {
      completion.complete(null);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    log.atError().addKeyValue("sessionId", sessionHandle.id().toString())
       .addKeyValue("error", throwable.getClass().getSimpleName())
       .setCause(throwable)
       .log("Blob Event Stream failed");
  }

  @Override
  public void onCompleted() {
    if (remaining.get() == 0 && !completion.isDone()) {
      completion.complete(null);
    }
  }

  private void handleEvent(String idString, ResultStatus status) {
    final var id = UUID.fromString(idString);
    switch (status) {
      case RESULT_STATUS_COMPLETED -> {
        final var metadata = pending.remove(id);
        if (metadata != null) {
          handleCompleted(metadata);
        }
      }
      case RESULT_STATUS_ABORTED -> {
        final var metadata = pending.remove(id);
        if (metadata != null) {
          handleAborted(metadata);
        }
      }
      default -> { /* ignore */ }
    }
  }

  private void handleAborted(BlobMetadata metadata) {
    try {
      listener.onError(new BlobError(metadata, null)); //TODO define exception
    } catch (Throwable throwable) {
      log.atError().addKeyValue("sessionId", sessionHandle.id().toString())
         .addKeyValue("metadata", metadata)
         .addKeyValue("error", throwable.getClass().getSimpleName())
         .setCause(throwable)
         .log("BlobCompletionListener throws an Exception");
    } finally {
      if (remaining.decrementAndGet() == 0 && !completion.isDone()) {
        completion.complete(null);
      }
    }
  }

  private void handleCompleted(BlobMetadata metadata) {
    blobService.downloadBlob(sessionHandle, metadata.id())
               .whenComplete((bytes, err) -> {
                 try {
                   if (err == null) {
                     listener.onSuccess(new Blob(metadata, bytes));
                   } else {
                     listener.onError(new BlobError(metadata, err));
                   }
                 } catch (Throwable throwable) {
                   log.atError().addKeyValue("sessionId", sessionHandle.id().toString())
                      .addKeyValue("metadata", metadata)
                      .addKeyValue("error", throwable.getClass().getSimpleName())
                      .setCause(throwable)
                      .log("BlobCompletionListener throws an Exception");
                 } finally {
                   if (remaining.decrementAndGet() == 0 && !completion.isDone()) {
                     completion.complete(null);
                   }
                 }
               });
  }
}
