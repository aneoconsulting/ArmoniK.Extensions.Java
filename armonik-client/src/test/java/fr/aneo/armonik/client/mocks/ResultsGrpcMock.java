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
package fr.aneo.armonik.client.mocks;

import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.*;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.protobuf.ByteString.*;
import static java.util.UUID.randomUUID;
import static java.util.stream.IntStream.range;

public class ResultsGrpcMock extends ResultsGrpc.ResultsImplBase {
  private UUID downloadFailedId;
  private UUID downloadSuccessId;
  private byte[] downloadContent;

  boolean firstCall = true;
  public String sessionId;
  public String blobId;
  public ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
  public List<Integer> dataChunkSizes = new ArrayList<>();


  @Override
  public void createResultsMetaData(CreateResultsMetaDataRequest request, StreamObserver<CreateResultsMetaDataResponse> responseObserver) {
    var builder = CreateResultsMetaDataResponse.newBuilder();
    range(0, request.getResultsCount()).forEach(i -> builder.addResults(ResultRaw.newBuilder().setResultId(randomUUID().toString())));
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }


  public void failDownloadFor(UUID id) {
    this.downloadFailedId = id;
  }

  public void setDownloadContentFor(UUID id, byte[] bytes) {
    this.downloadSuccessId = id;
    this.downloadContent = bytes;
  }

  @Override
  public void downloadResultData(DownloadResultDataRequest request, StreamObserver<DownloadResultDataResponse> responseObserver) {
    var id = UUID.fromString(request.getResultId());
    if (downloadFailedId != null && downloadFailedId.equals(id)) {
      responseObserver.onError(new RuntimeException("Failed to download result"));
      return;
    }

    byte[] bytes = ((downloadSuccessId != null) && downloadSuccessId.equals(id)) ? downloadContent : ("data-" + id).getBytes(StandardCharsets.UTF_8);

    responseObserver.onNext(DownloadResultDataResponse.newBuilder()
                                                      .setDataChunk(copyFrom(bytes))
                                                      .build());
    responseObserver.onCompleted();
  }


  @Override
  public StreamObserver<UploadResultDataRequest> uploadResultData(StreamObserver<UploadResultDataResponse> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(UploadResultDataRequest request) {
        if (firstCall && request.hasId()) {
          sessionId = request.getId().getSessionId();
          blobId = request.getId().getResultId();
          firstCall = false;
          return;
        }
        if (request.hasDataChunk()) {
          try {
            dataChunkSizes.add(request.getDataChunk().size());
            receivedData.write(request.getDataChunk().toByteArray());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public void onError(Throwable t) {
        // no-op for test
      }

      @Override
      public void onCompleted() {
        responseObserver.onNext(UploadResultDataResponse.getDefaultInstance());
        responseObserver.onCompleted();
      }
    };
  }
}
