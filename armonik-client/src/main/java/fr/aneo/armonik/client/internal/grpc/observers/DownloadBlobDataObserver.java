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
import fr.aneo.armonik.client.BlobHandle;
import fr.aneo.armonik.client.BlobId;
import fr.aneo.armonik.client.SessionId;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
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
  /** Largest array the JVM reliably allocates: a few words below {@code Integer.MAX_VALUE} are taken by the array header. */
  private static final long MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private final SettableFuture<byte[]> result = SettableFuture.create();
  /** Chunks as received, kept until the end so the data is copied once, into an array of the exact size. */
  private final List<ByteString> chunks = new ArrayList<>();
  private long size;
  private final SessionId sessionId;
  private final BlobId blobId;
  private final long maxSize;

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
    this(sessionId, blobId, MAX_ARRAY_SIZE);
  }

  /**
   * Creates a download observer that fails once the data exceeds {@code maxSize} bytes, for tests.
   */
  DownloadBlobDataObserver(SessionId sessionId, BlobId blobId, long maxSize) {
    this.sessionId = sessionId;
    this.blobId = blobId;
    this.maxSize = maxSize;
  }

  @Override
  public void onNext(DownloadResultDataResponse chunk) {
    if (result.isDone()) return;
    var data = chunk.getDataChunk();
    size += data.size();
    if (size > maxSize) {
      // Fail now and drop what was received, rather than keep the data until the stream ends
      chunks.clear();
      result.setException(new IllegalStateException("Blob " + blobId.asString() + " of session " + sessionId.asString()
        + " is over " + maxSize + " bytes, larger than a Java array can hold"));
      return;
    }
    chunks.add(data);
  }

  @Override
  public void onError(Throwable throwable) {
    logger.error("Blob download failed: operation={}, sessionId={}, blobId={}, error={}",
      "downloadBlob", sessionId.asString(), blobId.asString(), throwable.getClass().getSimpleName(),
      throwable);
    chunks.clear();
    result.setException(throwable);
  }

  @Override
  public void onCompleted() {
    logger.debug("Blob download completed: operation={}, sessionId={}, blobId={}, downloadSize={}",
      "downloadBlob", sessionId.asString(), blobId.asString(), size);
    if (result.isDone()) return;
    var data = new byte[(int) size];
    var offset = 0;
    for (var chunk : chunks) {
      chunk.copyTo(data, offset);
      offset += chunk.size();
    }
    chunks.clear();
    result.set(data);
  }

  public CompletionStage<byte[]> content() {
    return Futures.toCompletionStage(result);
  }
}
