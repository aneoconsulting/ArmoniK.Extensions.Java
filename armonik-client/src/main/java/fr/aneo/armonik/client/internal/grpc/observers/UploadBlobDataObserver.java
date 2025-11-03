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

import fr.aneo.armonik.client.definition.blob.BlobData;
import fr.aneo.armonik.client.model.BlobHandle;
import fr.aneo.armonik.client.model.BlobId;
import fr.aneo.armonik.client.model.SessionId;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
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
 * The observer is used internally by {@link BlobHandle#uploadData(BlobData)} and should
 * not be used directly by client applications. It handles the low-level gRPC streaming protocol
 * for efficient blob data transmission with flow control and error handling.
 * <p>
 * The implementation uses a drain-based approach to manage back pressure, ensuring that data
 * is sent only when the stream is ready to accept more content.
 *
 * @see BlobHandle#uploadData(BlobData)
 * @see DownloadBlobDataObserver
 * @see ClientResponseObserver
 */
public final class UploadBlobDataObserver implements ClientResponseObserver<UploadResultDataRequest, UploadResultDataResponse> {
  private static final Logger logger = LoggerFactory.getLogger(UploadBlobDataObserver.class);

  private final CompletableFuture<Void> completion = new CompletableFuture<>();
  private ClientCallStreamObserver<UploadResultDataRequest> request;
  private final SessionId sessionId;
  private final BlobId blobId;
  private final BlobData blobData;
  private boolean headerSent = false;
  private InputStream inputStream;
  private final byte[] buffer;
  private boolean streamCompleted = false;
  private final AtomicBoolean draining = new AtomicBoolean(false);

  /**
   * Creates a new upload observer for the specified blob with chunked data transmission.
   * <p>
   * The observer will upload the blob definition data in chunks of the specified size,
   * managing flow control and back pressure to ensure efficient transmission.
   *
   * @param sessionId the identifier of the session where the blob will be stored
   * @param blobId the identifier of the blob being uploaded
   * @param blobData the blob definition containing the data to upload
   * @param chunkSize the size of individual data chunks for transmission; must be positive
   * @throws NullPointerException if any parameter is null
   * @throws IllegalArgumentException if chunkSize is not positive
   * @see SessionId
   * @see BlobId
   * @see BlobData
   */
  public UploadBlobDataObserver(SessionId sessionId, BlobId blobId, BlobData blobData, int chunkSize) {
    this.sessionId = sessionId;
    this.blobId = blobId;
    this.blobData = blobData;
    this.buffer = new byte[chunkSize];
  }

  /**
   * Called before the stream starts. Initializes the request stream observer
   * and sets up the drain callback for flow control.
   *
   * @param requestStream the client stream observer for sending requests
   */
  @Override
  public void beforeStart(ClientCallStreamObserver<UploadResultDataRequest> requestStream) {
    this.request = requestStream;
    logger.debug("Upload stream started. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString());

    completion.whenComplete((unused, throwable) -> {
      closeStream();
      if (throwable instanceof CancellationException) {
        requestStream.cancel("client cancelled upload", throwable);
      }
    });
    requestStream.setOnReadyHandler(this::drain);
  }

  /**
   * Handles server responses during the upload operation.
   * Typically called when the server acknowledges received data.
   *
   * @param value the response from the server
   */
  @Override
  public void onNext(UploadResultDataResponse value) {
    logger.trace("Received server acknowledgment. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString());
  }

  /**
   * Handles errors that occur during the upload operation.
   * Completes the upload future exceptionally and closes the input stream.
   *
   * @param t the error that occurred
   */
  @Override
  public void onError(Throwable t) {
    logger.error("Upload stream error. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString(), t);
    closeStream();
    completion.completeExceptionally(t);
  }

  /**
   * Called when the server completes the upload response stream.
   * Marks the upload operation as successfully completed.
   */
  @Override
  public void onCompleted() {
    logger.debug("Upload stream completed successfully. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString());
    closeStream();
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
      // Open stream on first drain
      if (inputStream == null) {
        try {
          inputStream = blobData.stream();
        } catch (IOException e) {
          logger.error("Failed to open blob stream. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString(), e);
          request.onError(e);
          completion.completeExceptionally(e);
          return;
        }
      }

      // Send header if not sent yet
      if (!headerSent && request.isReady()) {
        sendIds();
      }

      // Stream chunks while there is demand
      while (headerSent && request.isReady() && !streamCompleted) {
        try {
          int bytesRead = inputStream.read(buffer);
          if (bytesRead == -1) {
            streamCompleted = true;
            break;
          }
          var chunk = toUploadResultDataRequest(buffer, 0, bytesRead);
          request.onNext(chunk);
        } catch (IOException e) {
          logger.error("Upload stream read error. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString(), e);
          request.onError(e);
          completion.completeExceptionally(e);
          return;
        }
      }

      if (headerSent && streamCompleted && request.isReady()) {
        request.onCompleted();
      }
    } catch (RuntimeException e) {
      logger.error("Upload stream drain error. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString(), e);
      request.onError(e);
      completion.completeExceptionally(e);
    } finally {
      draining.set(false);
      if (request.isReady() && !streamCompleted) {
        drain();
      }
    }
  }

  private void sendIds() {
    request.onNext(toUploadResultDataIdentifierRequest(sessionId, blobId));
    headerSent = true;
  }

  private void closeStream() {
    if (inputStream != null) {
      try {
        inputStream.close();
        logger.trace("Blob input stream closed. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString());
      } catch (IOException e) {
        logger.warn("Failed to close blob input stream. BlobId: {}, SessionId: {}", blobId.asString(), sessionId.asString(), e);
      }
    }
  }
}
