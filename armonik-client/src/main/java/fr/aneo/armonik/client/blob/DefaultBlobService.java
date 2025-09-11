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
import com.google.protobuf.ByteString;
import fr.aneo.armonik.client.session.SessionHandle;
import fr.aneo.armonik.client.util.FutureAdapters;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.*;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc.*;
import static java.util.Objects.requireNonNull;

public class DefaultBlobService implements BlobService {
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

    var resultRaws = FutureAdapters.toCompletionStage(resultsFutureStub.createResults(blobsRequest(sessionHandle, blobDefinitions)))
                                   .thenApply(CreateResultsResponse::getResultsList);
    return IntStream.range(0, blobDefinitions.size())
                    .mapToObj(toBlobHandle(sessionHandle, resultRaws))
                    .toList();
  }

  public CompletionStage<byte[]> downloadBlob(SessionHandle sessionHandle, UUID blobId) {
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
        result.setException(t);
      }

      @Override
      public void onCompleted() {
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
                                                                            .setData(ByteString.copyFrom(def.data()))
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
