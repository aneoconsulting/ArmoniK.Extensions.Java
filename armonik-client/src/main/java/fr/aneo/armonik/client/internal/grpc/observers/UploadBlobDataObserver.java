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

import fr.aneo.armonik.client.definition.BlobDefinition;
import fr.aneo.armonik.client.model.BlobHandle;
import fr.aneo.armonik.client.model.BlobId;
import fr.aneo.armonik.client.model.SessionId;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataRequest;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataResponse;
import static fr.aneo.armonik.client.internal.grpc.mappers.BlobMapper.toUploadResultDataIdentifierRequest;
import static fr.aneo.armonik.client.internal.grpc.mappers.BlobMapper.toUploadResultDataRequest;

/**
 * Internal gRPC stream observer for handling blob data upload operations to the ArmoniK cluster.
 * <p>
 * This class implements the {@link ClientResponseObserver} interface to handle bidirectional
 * streaming during blob data upload operations. It manages chunked data transmission with
 * proper back pressure control to prevent overwhelming the server or network.
 * <p>
 * The observer is used internally by {@link BlobHandle#uploadData(BlobDefinition)} and should
 * not be used directly by client applications. It handles the low-level gRPC streaming protocol
 * for efficient blob data transmission with flow control and error handling.
 * <p>
 * The implementation uses a drain-based approach to manage back pressure, ensuring that data
 * is sent only when the stream is ready to accept more content.
 *
 * @see BlobHandle#uploadData(BlobDefinition)
 * @see DownloadBlobDataObserver
 * @see ClientResponseObserver
 */
public class UploadBlobDataObserver implements ClientResponseObserver<UploadResultDataRequest, UploadResultDataResponse> {
  private static final Logger logger = LoggerFactory.getLogger(UploadBlobDataObserver.class);

  private final CompletableFuture<Void> completion = new CompletableFuture<>();
  private ClientCallStreamObserver<UploadResultDataRequest> request;
  private final SessionId sessionId;
  private final BlobId blobId;
  private int offset = 0;
  private boolean headerSent = false;
  private final byte[] data;
  private final int chunkSize;
  private final AtomicBoolean draining = new AtomicBoolean(false);

  /**
   * Creates a new upload observer for the specified blob with chunked data transmission.
   * <p>
   * The observer will upload the blob definition data in chunks of the specified size,
   * managing flow control and back pressure to ensure efficient transmission.
   *
   * @param sessionId the identifier of the session where the blob will be stored
   * @param blobId the identifier of the blob being uploaded
   * @param blobDefinition the blob definition containing the data to upload
   * @param chunkSize the size of individual data chunks for transmission
   * @throws NullPointerException if any parameter is null
   * @see SessionId
   * @see BlobId
   * @see BlobDefinition
   */
  public UploadBlobDataObserver(SessionId sessionId, BlobId blobId, BlobDefinition blobDefinition, int chunkSize) {
    this.sessionId = sessionId;
    this.blobId = blobId;
    this.data = blobDefinition.data();
    this.chunkSize = chunkSize;
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<UploadResultDataRequest> requestStream) {
    this.request = requestStream;
    logger.atDebug()
          .addKeyValue("sessionId", sessionId.asString())
          .addKeyValue("blobId", blobId.asString())
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
          .addKeyValue("sessionId", sessionId.asString())
          .addKeyValue("blobId", blobId.asString())
          .addKeyValue("phase", "serverAck")
          .log("Received server acknowledgment");
  }

  @Override
  public void onError(Throwable t) {
    logger.atError()
          .addKeyValue("sessionId", sessionId.asString())
          .addKeyValue("blobId", blobId.asString())
          .addKeyValue("error", t.getClass().getSimpleName())
          .setCause(t)
          .log("Upload stream failed");

    completion.completeExceptionally(t);
  }

  @Override
  public void onCompleted() {
    logger.atDebug()
          .addKeyValue("operation", "uploadStream")
          .addKeyValue("sessionId", sessionId.asString())
          .addKeyValue("blobId", blobId.asString())
          .addKeyValue("totalSize", data.length)
          .log("Upload stream completed successfully");

    completion.complete(null);
  }

  /**
   * Returns a completion future that indicates when the upload operation finishes.
   * <p>
   * The returned future completes successfully when the blob data has been fully
   * uploaded and acknowledged by the server. It completes exceptionally if the
   * upload fails due to network errors or other issues.
   *
   * @return a completion future for the upload operation
   */
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
        var chunk = toUploadResultDataRequest(data, offset, size);
        request.onNext(chunk);
        offset += size;
      }

      if (headerSent && offset >= data.length && request.isReady()) {
        request.onCompleted();
      }
    } catch (RuntimeException e) {
      logger.atError()
            .addKeyValue("sessionId", sessionId.asString())
            .addKeyValue("blobId", blobId.asString())
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
    request.onNext(toUploadResultDataIdentifierRequest(sessionId, blobId));
    headerSent = true;
  }
}
