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

import com.google.common.util.concurrent.SettableFuture;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import fr.aneo.armonik.client.model.BlobHandle;
import fr.aneo.armonik.client.model.BlobId;
import fr.aneo.armonik.client.model.SessionId;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.CompletionStage;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.DownloadResultDataResponse;

/**
 * Internal gRPC stream observer for handling blob data download operations from the ArmoniK cluster.
 * <p>
 * This class implements the {@link StreamObserver} interface to handle server-side streaming
 * responses during blob data download operations. It buffers incoming data chunks and provides
 * a completion stage that resolves with the complete blob content when the download finishes.
 * <p>
 * The observer is used internally by {@link BlobHandle#downloadData()} and should not be
 * used directly by client applications. It handles the low-level gRPC streaming protocol
 * for efficient blob data retrieval.
 *
 * @see BlobHandle#downloadData()
 * @see UploadBlobDataObserver
 * @see StreamObserver
 */
public class DownloadBlobDataObserver implements StreamObserver<DownloadResultDataResponse> {
  private static final Logger logger = LoggerFactory.getLogger(DownloadBlobDataObserver.class);

  private final SettableFuture<byte[]> result = SettableFuture.create();
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(8 * 1024);
  private final SessionId sessionId;
  private final BlobId blobId;

  /**
   * Creates a new download observer for the specified blob within a session context.
   * <p>
   * The observer will accumulate data chunks received from the gRPC stream and provide
   * the complete blob content through the {@link #content()} completion stage.
   *
   * @param sessionId the identifier of the session containing the blob
   * @param blobId the identifier of the blob to download
   * @throws NullPointerException if any parameter is null
   * @see SessionId
   * @see BlobId
   */
  public DownloadBlobDataObserver(SessionId sessionId, BlobId blobId) {
    this.sessionId = sessionId;
    this.blobId = blobId;
  }

  @Override
  public void onNext(DownloadResultDataResponse chunk) {
    try {
      buffer.write(chunk.getDataChunk().toByteArray());
    } catch (Exception e) {
      onError(e);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    logger.atError()
          .addKeyValue("operation", "downloadBlob")
          .addKeyValue("sessionId", sessionId.asString())
          .addKeyValue("blobId", blobId.asString())
          .addKeyValue("error", throwable.getClass().getSimpleName())
          .setCause(throwable)
          .log("Blob download failed");

    result.setException(throwable);
  }

  @Override
  public void onCompleted() {
    logger.atDebug()
          .addKeyValue("operation", "downloadBlob")
          .addKeyValue("sessionId", sessionId.asString())
          .addKeyValue("blobId", blobId.asString())
          .addKeyValue("downloadSize", buffer.size())
          .log("Blob download completed");

    result.set(buffer.toByteArray());
  }

  public CompletionStage<byte[]> content() {
    return Futures.toCompletionStage(result);
  }
}
