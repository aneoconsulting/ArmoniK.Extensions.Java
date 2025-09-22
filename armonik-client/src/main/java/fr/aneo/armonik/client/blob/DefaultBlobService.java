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

import com.google.common.util.concurrent.SettableFuture;
import fr.aneo.armonik.client.session.SessionHandle;
import fr.aneo.armonik.client.util.FutureAdapters;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static com.google.protobuf.ByteString.copyFrom;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.*;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc.*;
import static java.util.Objects.requireNonNull;

public class DefaultBlobService implements BlobService {
  private static final Logger logger = LoggerFactory.getLogger(DefaultBlobService.class);

  public static final int UPLOAD_CHUNK_SIZE = 84_000; // as define in ArmoniK server

  private final ResultsFutureStub resultsFutureStub;
  private final ResultsStub resultsStub;

  public DefaultBlobService(ManagedChannel channel) {
    this.resultsFutureStub = newFutureStub(channel);
    this.resultsStub = newStub(channel);
  }

  @Override
  public List<BlobHandle> createBlobMetaData(SessionHandle sessionHandle, int count) {
    requireNonNull(sessionHandle, "session must not be null");
    if (count < 0) throw new IllegalArgumentException("count must be positive");

    logger.atDebug()
          .addKeyValue("operation", "createBlobMetaData")
          .addKeyValue("sessionId", sessionHandle.id().toString())
          .addKeyValue("count", count)
          .log("Creating blob metadata");

    var resultRaws = FutureAdapters.toCompletionStage(resultsFutureStub.createResultsMetaData(blobsMetaDataRequest(sessionHandle, count)))
                                   .thenApply(CreateResultsMetaDataResponse::getResultsList);

    return IntStream.range(0, count)
                    .mapToObj(toBlobHandle(sessionHandle, resultRaws))
                    .toList();
  }

  @Override
  public List<BlobHandle> createBlobs(SessionHandle sessionHandle, List<BlobDefinition> blobDefinitions) {
    requireNonNull(sessionHandle, "sessionHandle must not be null");
    requireNonNull(blobDefinitions, "blobDefinitions must not be null");

    long totalSize = blobDefinitions.stream().mapToLong(def -> def.data().length).sum();

    logger.atDebug()
          .addKeyValue("operation", "createBlobs")
          .addKeyValue("sessionId", sessionHandle.id().toString())
          .addKeyValue("count", blobDefinitions.size())
          .addKeyValue("totalSize", totalSize)
          .log("Creating blobs with data");


    var resultRaws = FutureAdapters.toCompletionStage(resultsFutureStub.createResults(blobsRequest(sessionHandle, blobDefinitions)))
                                   .thenApply(CreateResultsResponse::getResultsList);
    return IntStream.range(0, blobDefinitions.size())
                    .mapToObj(toBlobHandle(sessionHandle, resultRaws))
                    .toList();
  }

  @Override
  public CompletionStage<Void> uploadBlobData(BlobHandle blobHandle, BlobDefinition blobDefinition) {
    requireNonNull(blobHandle, "handle must not be null");
    requireNonNull(blobDefinition, "blobDefinition must not be null");

    logger.atDebug()
          .addKeyValue("operation", "uploadBlobData")
          .addKeyValue("sessionId", blobHandle.sessionHandle().id().toString())
          .addKeyValue("blobSize", blobDefinition.data().length)
          .log("Starting blob upload");

    return blobHandle.metadata()
                 .thenCompose(metadata -> {
                   var uploadObserver = new UploadBlobDataObserver(blobHandle.sessionHandle(), metadata.id(), blobDefinition, UPLOAD_CHUNK_SIZE);
                   //noinspection ResultOfMethodCallIgnored
                   resultsStub.uploadResultData(uploadObserver);
                   return uploadObserver.completion();
                 });
  }

  public CompletionStage<byte[]> downloadBlob(SessionHandle sessionHandle, UUID blobId) {
    logger.atDebug()
          .addKeyValue("operation", "downloadBlob")
          .addKeyValue("sessionId", sessionHandle.id().toString())
          .addKeyValue("blobId", blobId.toString())
          .log("Starting blob download");

    var request = DownloadResultDataRequest.newBuilder()
                                           .setSessionId(sessionHandle.id().toString()).setResultId(blobId.toString())
                                           .build();

    var result = SettableFuture.<byte[]>create();
    var buffer = new ByteArrayOutputStream(8 * 1024);

    resultsStub.downloadResultData(request, new StreamObserver<>() {
      @Override
      public void onNext(DownloadResultDataResponse chunk) {
        try {
          buffer.write(chunk.getDataChunk().toByteArray());
        } catch (Exception e) {
          onError(e);
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.atError()
              .addKeyValue("operation", "downloadBlob")
              .addKeyValue("sessionId", sessionHandle.id().toString())
              .addKeyValue("blobId", blobId.toString())
              .addKeyValue("error", t.getClass().getSimpleName())
              .setCause(t)
              .log("Blob download failed");

        result.setException(t);
      }

      @Override
      public void onCompleted() {
        logger.atDebug()
              .addKeyValue("operation", "downloadBlob")
              .addKeyValue("sessionId", sessionHandle.id().toString())
              .addKeyValue("blobId", blobId.toString())
              .addKeyValue("downloadSize", buffer.size())
              .log("Blob download completed");

        result.set(buffer.toByteArray());
      }
    });

    return FutureAdapters.toCompletionStage(result);
  }

  private static CreateResultsMetaDataRequest blobsMetaDataRequest(SessionHandle sessionHandle, int count) {
    return CreateResultsMetaDataRequest.newBuilder()
                                       .setSessionId(sessionHandle.id().toString())
                                       .addAllResults(IntStream.range(0, count)
                                                               .mapToObj(index -> CreateResultsMetaDataRequest.ResultCreate.newBuilder().build())
                                                               .toList())
                                       .build();
  }

  private static CreateResultsRequest blobsRequest(SessionHandle sessionHandle, List<BlobDefinition> blobDefinitions) {
    var blobs = blobDefinitions.stream()
                               .map(def -> CreateResultsRequest.ResultCreate.newBuilder()
                                                                            .setData(copyFrom(def.data()))
                                                                            .build())
                               .toList();

    return CreateResultsRequest.newBuilder()
                               .setSessionId(sessionHandle.id().toString())
                               .addAllResults(blobs)
                               .build();
  }

  private static IntFunction<BlobHandle> toBlobHandle(SessionHandle sessionHandle, CompletionStage<List<ResultRaw>> resultRaws) {
    return index -> new BlobHandle(
      sessionHandle,
      resultRaws.thenApply(resultRaw -> new BlobMetadata(UUID.fromString(resultRaw.get(index).getResultId()))
      )
    );
  }
}
