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

import fr.aneo.armonik.client.definition.BlobDefinition;
import fr.aneo.armonik.client.internal.grpc.observers.DownloadBlobDataObserver;
import fr.aneo.armonik.client.internal.grpc.observers.UploadBlobDataObserver;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc.ResultsStub;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc.newStub;
import static fr.aneo.armonik.client.internal.grpc.mappers.BlobMapper.toDownloadResultDataRequest;
import static java.util.Objects.requireNonNull;

/**
 * Handle representing a blob within the ArmoniK distributed computing platform.
 * <p>
 * A blob is the generic term for data inputs and outputs of tasks in ArmoniK. This handle
 * provides a lightweight reference to blob data stored in the ArmoniK cluster and offers
 * direct upload and download capabilities through gRPC interactions.
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
 * @see SessionHandle
 * @see TaskHandle
 */
public final class BlobHandle {
  private static final Logger logger = LoggerFactory.getLogger(BlobHandle.class);

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
  private final ResultsStub resultsStub;


  /**
   * Creates a new blob handle within the specified session context.
   * <p>
   * This constructor initializes the handle with the necessary components for
   * blob data operations. The blob information becomes available through the
   * deferred completion stage once the cluster assigns identifiers.
   *
   * @param sessionId        the identifier of the session that owns this blob
   * @param deferredBlobInfo a completion stage that will provide blob metadata when available
   * @param channel          the gRPC channel for communicating with the ArmoniK cluster
   * @throws NullPointerException if any parameter is null
   * @see BlobInfo
   * @see SessionId
   */
  BlobHandle(SessionId sessionId, CompletionStage<BlobInfo> deferredBlobInfo, ManagedChannel channel) {
    requireNonNull(sessionId, "sessionId must not be null");
    requireNonNull(deferredBlobInfo, "blobInfo");
    requireNonNull(channel, "chanel must not be null");

    this.sessionId = sessionId;
    this.deferredBlobInfo = deferredBlobInfo;
    this.resultsStub = newStub(channel);
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
   * This method transfers the data content specified in the blob definition to the
   * ArmoniK cluster. The upload is performed asynchronously, and
   * the returned completion stage completes when the entire data transfer is finished.
   *
   * @param blobDefinition the definition containing the data to upload
   * @return a completion stage that completes when the upload is finished
   * @throws NullPointerException if blobDefinition is null
   * @throws RuntimeException     if upload fails due to cluster communication issues
   * @see BlobDefinition
   */
  public CompletionStage<Void> uploadData(BlobDefinition blobDefinition) {
    requireNonNull(blobDefinition, "blobDefinition must not be null");

    logger.atDebug()
          .addKeyValue("operation", "uploadBlobData")
          .addKeyValue("sessionId", sessionId().asString())
          .addKeyValue("blobSize", blobDefinition.data().length)
          .log("Starting blob upload");

    return deferredBlobInfo().thenCompose(blobInfo -> {
      var uploadObserver = new UploadBlobDataObserver(sessionId(), blobInfo.id(), blobDefinition, UPLOAD_CHUNK_SIZE);
      //noinspection ResultOfMethodCallIgnored
      resultsStub.uploadResultData(uploadObserver);
      return uploadObserver.completion();
    });
  }

  /**
   * Downloads the complete data content of this blob from the ArmoniK cluster.
   * <p>
   * This method retrieves the entire blob content from the cluster and returns it
   * as a byte array. The download is performed asynchronously and the returned
   * completion stage completes when all data has been received.
   *
   * @return a completion stage that completes with the blob data as a byte array
   * @throws RuntimeException if download fails due to cluster communication issues
   * @see #uploadData(BlobDefinition)
   */
  public CompletionStage<byte[]> downloadData() {
    return deferredBlobInfo.thenCompose(blobInfo -> {
      logger.atDebug()
            .addKeyValue("operation", "downloadBlob")
            .addKeyValue("sessionId", sessionId.asString())
            .addKeyValue("blobId", blobInfo.id().asString())
            .log("Starting blob download");

      var request = toDownloadResultDataRequest(sessionId(), blobInfo.id());
      var responseObserver = new DownloadBlobDataObserver(sessionId(), blobInfo.id());
      resultsStub.downloadResultData(request, responseObserver);

      return responseObserver.content();
    });

  }
}
