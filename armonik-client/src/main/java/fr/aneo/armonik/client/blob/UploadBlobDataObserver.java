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
package fr.aneo.armonik.client.blob;

import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataRequest.ResultIdentifier;
import fr.aneo.armonik.client.session.SessionHandle;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.protobuf.ByteString.copyFrom;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataRequest;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataResponse;

public class UploadBlobDataObserver implements ClientResponseObserver<UploadResultDataRequest, UploadResultDataResponse> {
  private static final Logger logger = LoggerFactory.getLogger(UploadBlobDataObserver.class);

  private final CompletableFuture<Void> completion = new CompletableFuture<>();
  private ClientCallStreamObserver<UploadResultDataRequest> request;
  private final SessionHandle sessionHandle;
  private final UUID blobId;
  private int offset = 0;
  private boolean headerSent = false;
  private final byte[] data;
  private final int chunkSize;
  private final AtomicBoolean draining = new AtomicBoolean(false);

  public UploadBlobDataObserver(SessionHandle sessionHandle, UUID blobId, BlobDefinition blobDefinition, int chunkSize) {
    this.sessionHandle = sessionHandle;
    this.blobId = blobId;
    this.data = blobDefinition.data();
    this.chunkSize = chunkSize;
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<UploadResultDataRequest> requestStream) {
    this.request = requestStream;

    logger.atDebug()
          .addKeyValue("sessionId", sessionHandle.id().toString())
          .addKeyValue("blobId", blobId.toString())
          .addKeyValue("totalSize", data.length)
          .log("Upload stream started");

    completion.whenComplete((unused, throwable) -> {
      if (throwable instanceof CancellationException) requestStream.cancel("client cancelled upload", throwable);
    });
    requestStream.setOnReadyHandler(this::drain);
  }

  @Override
  public void onNext(UploadResultDataResponse value) {
    /* no-op (server may ACK) */
    logger.atTrace()
          .addKeyValue("operation", "uploadStream")
          .addKeyValue("sessionId", sessionHandle.id().toString())
          .addKeyValue("blobId", blobId.toString())
          .addKeyValue("phase", "serverAck")
          .log("Received server acknowledgment");
  }

  @Override
  public void onError(Throwable t) {
    logger.atError()
          .addKeyValue("sessionId", sessionHandle.id().toString())
          .addKeyValue("blobId", blobId.toString())
          .addKeyValue("error", t.getClass().getSimpleName())
          .setCause(t)
          .log("Upload stream failed");

    completion.completeExceptionally(t);
  }

  @Override
  public void onCompleted() {
    logger.atDebug()
          .addKeyValue("operation", "uploadStream")
          .addKeyValue("sessionId", sessionHandle.id().toString())
          .addKeyValue("blobId", blobId.toString())
          .addKeyValue("totalSize", data.length)
          .log("Upload stream completed successfully");

    completion.complete(null);
  }

  public CompletableFuture<Void> completion() {
    return completion;
  }

  private void drain() {
    if (!draining.compareAndSet(false, true)) return;
    try {
      if (!headerSent && request.isReady()) {
        sendIds();
      }

      // Stream chunks while there is demand
      while (headerSent && request.isReady() && offset < data.length) {
        int size = Math.min(chunkSize, data.length - offset);
        var chunk = UploadResultDataRequest.newBuilder()
                                           .setDataChunk(copyFrom(data, offset, size))
                                           .build();
        request.onNext(chunk);
        offset += size;
      }

      // Complete when all data has been sent
      if (headerSent && offset >= data.length && request.isReady()) {
        request.onCompleted();
      }
    } catch (RuntimeException e) {
      logger.atError()
            .addKeyValue("sessionId", sessionHandle.id().toString())
            .addKeyValue("blobId", blobId.toString())
            .addKeyValue("error", e.getClass().getSimpleName())
            .setCause(e)
            .log("Upload stream drain error");

      request.onError(e);
      completion.completeExceptionally(e);
    } finally {
      draining.set(false);
      if (request.isReady() && offset < data.length) {
        drain();
      }
    }
  }

  private void sendIds() {
    var first = UploadResultDataRequest.newBuilder()
                                       .setId(ResultIdentifier.newBuilder()
                                                              .setResultId(blobId.toString())
                                                              .setSessionId(sessionHandle.id().toString()))
                                       .build();
    request.onNext(first);
    headerSent = true;
  }
}
