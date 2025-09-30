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
package fr.aneo.armonik.client.internal.grpc.observers;

import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse;
import fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus;
import fr.aneo.armonik.client.model.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static fr.aneo.armonik.client.model.BlobCompletionListener.Blob;
import static fr.aneo.armonik.client.model.BlobCompletionListener.BlobError;

/**
 * Internal gRPC stream observer that processes blob completion events and manages listener notifications.
 * <p>
 * This class implements the {@link StreamObserver} contract to handle server-side streaming events
 * from the ArmoniK Control Plane's Events API. It serves as the bridge between the raw gRPC event
 * stream and the higher-level {@link BlobCompletionListener} interface, providing event filtering,
 * state management, and automatic blob data retrieval.

 * <p>
 * For each incoming event, the observer filters events to only those matching watched blob IDs,
 * processes COMPLETED events by automatically downloading blob data, processes ABORTED events
 * by notifying about failures, and tracks completion progress.
 * <p>
 * This class is thread-safe and handles listener exceptions gracefully to prevent stream disruption.
 *
 * @see BlobCompletionListener
 * @see StreamObserver
 */
public final class BlobCompletionEventObserver implements StreamObserver<EventSubscriptionResponse> {

  private static final Logger log = LoggerFactory.getLogger(BlobCompletionEventObserver.class);

  private final SessionId sessionId;
  private final ConcurrentMap<String, BlobHandle> pending;
  private final AtomicInteger remaining;
  private final CompletableFuture<Void> completion;
  private final BlobCompletionListener listener;

  /**
   * Creates a new blob completion event observer for the specified session and blob handles.
   * <p>
   * The observer will monitor the provided blob handles for completion events and notify
   * the listener when blobs become available or encounter errors. The completion future
   * will be completed when all tracked blobs reach terminal states.
   *
   * @param sessionId the identifier of the session containing the blobs to monitor
   * @param pending a map of blob handles to monitor, keyed by their blob identifiers
   * @param completion the future to complete when all blobs reach terminal states
   * @param listener the listener to receive blob completion notifications
   * @throws NullPointerException if any parameter is null
   * @see SessionId
   * @see BlobHandle
   * @see BlobCompletionListener
   */
  public BlobCompletionEventObserver(SessionId sessionId,
                                     Map<BlobId, BlobHandle> pending,
                                     CompletableFuture<Void> completion,
                                     BlobCompletionListener listener) {
    this.sessionId = sessionId;
    this.pending = pending.entrySet().stream().collect(Collectors.toConcurrentMap(entry -> entry.getKey().asString(), Map.Entry::getValue));
    this.remaining = new AtomicInteger(pending.size());
    this.completion = completion;
    this.listener = listener;
  }

  @Override
  public void onNext(EventSubscriptionResponse resp) {
    switch (resp.getUpdateCase()) {
      case RESULT_STATUS_UPDATE ->
        handleEvent(resp.getResultStatusUpdate().getResultId(), resp.getResultStatusUpdate().getStatus());
      case NEW_RESULT ->
        handleEvent(resp.getNewResult().getResultId(), resp.getNewResult().getStatus());
      case UPDATE_NOT_SET ->
      { /* ignore */ }
    }

    if (remaining.get() == 0 && !completion.isDone()) {
      completion.complete(null);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    log.atError().addKeyValue("sessionId", sessionId.asString())
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

  private void handleEvent(String blobId, ResultStatus status) {
    switch (status) {
      case RESULT_STATUS_COMPLETED -> {
        final var blobHandle = pending.remove(blobId);
        if (blobHandle != null) {
          handleCompleted(blobHandle);
        }
      }
      case RESULT_STATUS_ABORTED -> {
        final var blobHandle = pending.remove(blobId);
        if (blobHandle != null) {
          handleAborted(blobHandle);
        }
      }
      default -> { /* ignore */ }
    }
  }

  private void handleAborted(BlobHandle blobHandle) {
    blobHandle.deferredBlobInfo()
              .thenAccept(blobInfo -> {
                try {
                  listener.onError(new BlobError(blobInfo, null)); //TODO define exception
                } catch (Throwable throwable) {
                  log.atError().addKeyValue("sessionId", sessionId.asString())
                     .addKeyValue("blobInfo", blobInfo)
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

  private void handleCompleted(BlobHandle blobHandle) {
    blobHandle.deferredBlobInfo().thenCompose(blobInfo ->
      blobHandle.downloadData()
                .whenComplete((bytes, err) -> {
        try {
          if (err == null) {
            listener.onSuccess(new Blob(blobInfo, bytes));
          } else {
            listener.onError(new BlobError(blobInfo, err));
          }
        } catch (Throwable throwable) {
          log.atError()
             .addKeyValue("sessionId", sessionId.asString())
             .addKeyValue("blobInfo", blobInfo)
             .addKeyValue("error", throwable.getClass().getSimpleName())
             .setCause(throwable)
             .log("BlobCompletionListener throws an Exception");
        } finally {
          if (remaining.decrementAndGet() == 0 && !completion.isDone()) {
            completion.complete(null);
          }
        }
      })
    );
  }
}
