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
package fr.aneo.armonik.client.testutils;

import com.google.protobuf.Timestamp;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.*;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.client.BlobId;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.protobuf.ByteString.copyFrom;
import static fr.aneo.armonik.client.TestDataFactory.blobId;

public class ResultsGrpcMock extends ResultsGrpc.ResultsImplBase {
  private BlobId downloadFailedId;
  private BlobId downloadSuccessId;
  private byte[] downloadContent;
  public List<UploadedDataInfo> uploadedDataInfos = new ArrayList<>();
  public List<MetadataRequest> metadataRequests = new ArrayList<>();


  public void failDownloadFor(BlobId id) {
    this.downloadFailedId = id;
  }

  public void setDownloadContentFor(BlobId id, byte[] bytes) {
    this.downloadSuccessId = id;
    this.downloadContent = bytes;
  }

  public void reset() {
    this.downloadFailedId = null;
    this.downloadSuccessId = null;
    this.downloadContent = null;
    this.uploadedDataInfos = new ArrayList<>();
    this.metadataRequests = new ArrayList<>();
  }

  @Override
  public void createResultsMetaData(CreateResultsMetaDataRequest request, StreamObserver<CreateResultsMetaDataResponse> responseObserver) {
    var builder = CreateResultsMetaDataResponse.newBuilder();
    metadataRequests.addAll(request.getResultsList()
                                   .stream()
                                   .map(metadata -> new MetadataRequest(request.getSessionId(), metadata.getName(), metadata.getManualDeletion()))
                                   .toList());


    builder.addAllResults(request.getResultsList()
                                 .stream()
                                 .map(metadata -> ResultRaw.newBuilder()
                                                           .setResultId(UUID.randomUUID().toString())
                                                           .setName(metadata.getName())
                                                           .setManualDeletion(metadata.getManualDeletion())
                                                           .setCreatedBy(UUID.randomUUID().toString())
                                                           .setCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                                                           .build())
                                 .toList());
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void downloadResultData(DownloadResultDataRequest request, StreamObserver<DownloadResultDataResponse> responseObserver) {
    var id = blobId(request.getResultId());
    if (downloadFailedId != null && downloadFailedId.equals(blobId(request.getResultId()))) {
      responseObserver.onError(new RuntimeException("Failed to download result"));
      return;
    }

    byte[] bytes = ((downloadSuccessId != null) && downloadSuccessId.equals(id))
      ? downloadContent
      : ("data-" + id).getBytes(StandardCharsets.UTF_8);
    responseObserver.onNext(DownloadResultDataResponse.newBuilder()
                                                      .setDataChunk(copyFrom(bytes))
                                                      .build());
    responseObserver.onCompleted();
  }


  @Override
  public StreamObserver<UploadResultDataRequest> uploadResultData(StreamObserver<UploadResultDataResponse> responseObserver) {
    return new StreamObserver<>() {
      private final UploadedDataInfo uploadedDataInfo = new UploadedDataInfo();

      @Override
      public void onNext(UploadResultDataRequest request) {
        if (uploadedDataInfo.firstCall && request.hasId()) {
          uploadedDataInfo.sessionId = request.getId().getSessionId();
          uploadedDataInfo.blobId = request.getId().getResultId();
          uploadedDataInfo.firstCall = false;
          return;
        }
        if (request.hasDataChunk()) {
          try {
            uploadedDataInfo.dataChunkSizes.add(request.getDataChunk().size());
            uploadedDataInfo.receivedData.write(request.getDataChunk().toByteArray());
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
        uploadedDataInfos.add(uploadedDataInfo);
      }
    };
  }

  public static class UploadedDataInfo {
    public String sessionId;
    public String blobId;
    public ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
    public List<Integer> dataChunkSizes = new ArrayList<>();

    private boolean firstCall = true;
  }

  public record MetadataRequest(String sessionId, String blobName, boolean manualDeletion) {
  }
}
