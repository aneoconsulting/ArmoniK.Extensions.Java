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
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.DownloadResultDataRequest;
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
public class DownloadBlobDataObserver implements ClientResponseObserver<DownloadResultDataRequest, DownloadResultDataResponse> {
  private static final Logger logger = LoggerFactory.getLogger(DownloadBlobDataObserver.class);
  /** Largest array the JVM reliably allocates: a few words below {@code Integer.MAX_VALUE} are taken by the array header. */
  private static final long MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private final SettableFuture<byte[]> result = SettableFuture.create();
  /** Chunks as received, when no array could be allocated up front; copied once, at the end, into an array of the exact size. */
  private final List<ByteString> chunks = new ArrayList<>();
  /** Array allocated up front from the expected size; null once a chunk does not fit, or when the size is unknown. */
  private byte[] preallocated;
  private int filled;
  private long size;
  private final SessionId sessionId;
  private final BlobId blobId;
  private final long maxSize;
  /** The call this observer receives, so that it can stop a download it gives up on. */
  private volatile ClientCallStreamObserver<DownloadResultDataRequest> call;

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
    this(sessionId, blobId, -1);
  }

  /**
   * Creates an observer that fills an array of {@code expectedSize} bytes as chunks arrive, so the
   * data is copied once and no chunk is kept. When the size is unknown, negative, or turns out wrong,
   * the observer keeps the chunks and assembles them at the end instead.
   *
   * @param sessionId    the identifier of the session containing the blob
   * @param blobId       the identifier of the blob to download
   * @param expectedSize the size of the data, or a negative value when unknown
   */
  public DownloadBlobDataObserver(SessionId sessionId, BlobId blobId, long expectedSize) {
    this(sessionId, blobId, expectedSize, MAX_ARRAY_SIZE);
  }

  /**
   * Creates an observer that fails once the data exceeds {@code maxSize} bytes, for tests.
   */
  DownloadBlobDataObserver(SessionId sessionId, BlobId blobId, long expectedSize, long maxSize) {
    this.sessionId = sessionId;
    this.blobId = blobId;
    this.maxSize = maxSize;
    if (expectedSize >= 0 && expectedSize <= maxSize) this.preallocated = new byte[(int) expectedSize];
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<DownloadResultDataRequest> requestStream) {
    this.call = requestStream;
  }

  @Override
  public void onNext(DownloadResultDataResponse chunk) {
    if (result.isDone()) return;
    var data = chunk.getDataChunk();
    size += data.size();
    if (size > maxSize) {
      // Fail now and drop what was received, rather than keep the data until the stream ends. The call is
      // cancelled before the result fails: the result returns the channel to the pool, which must not lend it
      // to another call while the server is still sending the rest of the blob.
      preallocated = null;
      chunks.clear();
      var call = this.call;
      if (call != null) call.cancel("Blob larger than a Java array can hold", null);
      result.setException(new IllegalStateException("Blob " + blobId.asString() + " of session " + sessionId.asString()
        + " is over " + maxSize + " bytes, larger than a Java array can hold"));
      return;
    }
    if (preallocated != null) {
      if (filled + data.size() <= preallocated.length) {
        data.copyTo(preallocated, filled);
        filled += data.size();
        return;
      }
      // More data than announced: keep what was received as a chunk and collect the rest.
      chunks.add(ByteString.copyFrom(preallocated, 0, filled));
      preallocated = null;
    }
    chunks.add(data);
  }

  @Override
  public void onError(Throwable throwable) {
    if (result.isDone()) {
      // The CANCELLED error that follows a download this observer gave up on
      logger.debug("Blob download stopped: sessionId={}, blobId={}", sessionId.asString(), blobId.asString());
      preallocated = null;
      chunks.clear();
      return;
    }
    logger.error("Blob download failed: operation={}, sessionId={}, blobId={}, error={}",
      "downloadBlob", sessionId.asString(), blobId.asString(), throwable.getClass().getSimpleName(),
      throwable);
    preallocated = null;
    chunks.clear();
    result.setException(throwable);
  }

  @Override
  public void onCompleted() {
    logger.debug("Blob download completed: operation={}, sessionId={}, blobId={}, downloadSize={}",
      "downloadBlob", sessionId.asString(), blobId.asString(), size);
    if (result.isDone()) return;
    if (preallocated != null) {
      result.set(filled == preallocated.length ? preallocated : java.util.Arrays.copyOf(preallocated, filled));
      preallocated = null;
      return;
    }
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
