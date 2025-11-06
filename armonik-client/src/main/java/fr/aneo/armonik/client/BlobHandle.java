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
package fr.aneo.armonik.client;

import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.client.definition.blob.BlobData;
import fr.aneo.armonik.client.definition.blob.BlobDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.client.internal.grpc.observers.DownloadBlobDataObserver;
import fr.aneo.armonik.client.internal.grpc.observers.UploadBlobDataObserver;
import fr.aneo.armonik.client.internal.retry.RetryableStreamOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

import static fr.aneo.armonik.client.internal.grpc.mappers.BlobMapper.toDownloadResultDataRequest;
import static java.util.Objects.requireNonNull;

/**
 * Handle representing a blob within the ArmoniK distributed computing platform.
 * <p>
 * A blob is the generic term for data inputs and outputs of tasks in ArmoniK. This handle
 * provides a lightweight reference to blob data stored in the ArmoniK cluster and offers
 * direct upload and download capabilities through gRPC interactions.
 * <p>
 * BlobHandles can be created in two ways:
 * <ul>
 *   <li><strong>Task-specific blobs:</strong> Created automatically during task submission for inputs and outputs</li>
 *   <li><strong>Session-scoped blobs:</strong> Created explicitly via {@link SessionHandle#createBlob(InputBlobDefinition)}
 *       for sharing across multiple tasks within the same session</li>
 * </ul>
 * <p>
 * BlobHandle instances are responsible for managing their own data lifecycle, including:
 * <ul>
 *   <li>Uploading data content to the cluster</li>
 *   <li>Downloading data content from the cluster</li>
 *   <li>Providing access to blob metadata asynchronously</li>
 * </ul>
 * <p>
 * The handle does not store blob content locally and performs all data operations
 * through non-blocking, asynchronous gRPC calls to the cluster.
 * <p>
 * This class is thread-safe and supports concurrent operations.
 *
 * @see BlobInfo
 * @see BlobDefinition
 * @see SessionHandle#createBlob(InputBlobDefinition)
 * @see TaskHandle
 */
public final class BlobHandle {
  private static final Logger log = LoggerFactory.getLogger(BlobHandle.class);

  /**
   * The chunk size used for uploading blob data to the ArmoniK cluster.
   * <p>
   * This constant defines the maximum size of individual data chunks sent during
   * upload operations. The value is aligned with ArmoniK server configuration
   * for optimal network performance.
   */
  public static final int UPLOAD_CHUNK_SIZE = 84_000;
  private final SessionId sessionId;
  private final CompletionStage<BlobInfo> deferredBlobInfo;
  private final ChannelPool channelPool;


  /**
   * Creates a new blob handle within the specified session context.
   * <p>
   * This constructor initializes the handle with the necessary components for
   * blob data operations. The blob information becomes available through the
   * deferred completion stage once the cluster assigns identifiers.
   *
   * @param sessionId        the identifier of the session that owns this blob
   * @param deferredBlobInfo a completion stage that will provide blob metadata when available
   * @param channelPool      the gRPC channel pool for cluster communication
   * @throws NullPointerException if any parameter is null
   * @see BlobInfo
   * @see SessionId
   */
  BlobHandle(SessionId sessionId, CompletionStage<BlobInfo> deferredBlobInfo, ChannelPool channelPool) {
    this.sessionId = requireNonNull(sessionId, "sessionId must not be null");
    this.deferredBlobInfo = requireNonNull(deferredBlobInfo, "deferredBlobInfo must no be null");
    this.channelPool = requireNonNull(channelPool, "chanelPool must not be null");
  }

  /**
   * Returns the identifier of the session that owns this blob.
   * <p>
   * All blobs belong to a session context that defines their scope and lifecycle.
   * Blobs can only be accessed within the context of their owning session.
   *
   * @return the session identifier
   * @see SessionId
   * @see SessionHandle
   */
  public SessionId sessionId() {
    return sessionId;
  }

  /**
   * Returns a completion stage that provides immutable metadata about this blob.
   * <p>
   * The blob information becomes available after the ArmoniK cluster acknowledges
   * the blob creation and assigns identifiers. The returned completion stage may
   * complete exceptionally if blob creation or metadata retrieval fails.
   * <p>
   * This method provides non-blocking access to blob metadata, including the
   * blob identifier and other cluster-assigned properties.
   *
   * @return a completion stage that completes with blob metadata
   * @see BlobInfo
   */
  public CompletionStage<BlobInfo> deferredBlobInfo() {
    return deferredBlobInfo;
  }

  /**
   * Uploads data to this blob in the ArmoniK cluster.
   * <p>
   * This method transfers the data content to the ArmoniK cluster using chunked streaming
   * for efficient network utilization. The upload is performed asynchronously with automatic
   * retry for transient failures if the blob data supports it.
   * <p>
   * If the blob data is retryable (see {@link BlobData#isRetryable()}), transient failures
   * such as network interruptions will trigger automatic retries with exponential backoff.
   * Non-retryable blob data will be attempted only once.
   *
   * @param blobData the data to upload
   * @return a completion stage that completes when the upload is finished
   * @throws NullPointerException if blobData is null
   * @throws RuntimeException     if upload fails due to cluster communication issues
   * @see BlobData
   * @see BlobData#isRetryable()
   */
  public CompletionStage<Void> uploadData(BlobData blobData) {
    requireNonNull(blobData, "blobData must not be null");

    if (!blobData.isRetryable()) {
      log.debug("BlobData is not retryable, attempting upload once. SessionId: {}", sessionId.asString());
      return deferredBlobInfo().thenCompose(info -> executeUpload(info, blobData));
    }

    log.debug("Starting retryable blob upload. SessionId: {}", sessionId.asString());

    return RetryableStreamOperation.execute(
      () -> deferredBlobInfo().thenCompose(info -> executeUpload(info, blobData)),
      channelPool.retryPolicy()
    );
  }

  private CompletionStage<Void> executeUpload(BlobInfo blobInfo, BlobData blobData) {
    return channelPool.executeAsync(channel -> {
      var resultsStub = ResultsGrpc.newStub(channel);
      var uploadObserver = new UploadBlobDataObserver(sessionId, blobInfo.id(), blobData, UPLOAD_CHUNK_SIZE);

      //noinspection ResultOfMethodCallIgnored
      resultsStub.uploadResultData(uploadObserver);

      return uploadObserver.completion();
    });
  }

  /**
   * Downloads the complete data content of this blob from the ArmoniK cluster.
   * <p>
   * This method retrieves the entire blob content from the cluster and returns it
   * as a byte array. The download is performed asynchronously with automatic retry
   * for transient failures such as network interruptions.
   * <p>
   * Transient failures will trigger automatic retries with exponential backoff
   * according to the configured retry policy.
   *
   * @return a completion stage that completes with the blob data as a byte array
   * @throws RuntimeException if download fails due to cluster communication issues
   * @see #uploadData(BlobData)
   */
  public CompletionStage<byte[]> downloadData() {
    return RetryableStreamOperation.execute(
      () -> deferredBlobInfo().thenCompose(this::executeDownload),
      channelPool.retryPolicy()
    );
  }

  private CompletionStage<byte[]> executeDownload(BlobInfo blobInfo) {
    return channelPool.executeAsync(channel -> {
      log.debug("Starting blob download. SessionId: {}, BlobId: {}", sessionId.asString(), blobInfo.id().asString());

      var resultsStub = ResultsGrpc.newStub(channel);
      var request = toDownloadResultDataRequest(sessionId, blobInfo.id());
      var responseObserver = new DownloadBlobDataObserver(sessionId, blobInfo.id());

      resultsStub.downloadResultData(request, responseObserver);

      return responseObserver.content();
    });
  }
}
