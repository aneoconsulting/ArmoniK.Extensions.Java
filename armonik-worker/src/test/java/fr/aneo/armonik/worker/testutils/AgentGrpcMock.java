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
package fr.aneo.armonik.worker.testutils;

import com.google.protobuf.Timestamp;
import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.*;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.CreateResultsRequest;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.CreateResultsResponse;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.*;
import static java.util.stream.Collectors.*;

public class AgentGrpcMock extends AgentGrpc.AgentImplBase {
  public UploadedBlobs uploadedBlobs;
  public BlobMetadata blobMetadata;
  public NotifyBlobs notifyBlobs;

  /**
   * Resets all captured data. Call this in @BeforeEach if reusing the mock.
   */
  public void reset() {
    uploadedBlobs = null;
    blobMetadata = null;
    notifyBlobs = null;
  }

  @Override
  public void notifyResultData(NotifyResultDataRequest request, StreamObserver<NotifyResultDataResponse> responseObserver) {
    if (notifyBlobs == null) {
      notifyBlobs = new NotifyBlobs(request.getCommunicationToken(), new HashMap<>());
    }

    notifyBlobs.ids().putAll(request.getIdsList().stream().collect(toMap(
      NotifyResultDataRequest.ResultIdentifier::getSessionId,
      NotifyResultDataRequest.ResultIdentifier::getResultId
    )));

    responseObserver.onNext(NotifyResultDataResponse.newBuilder()
                                                    .addAllResultIds(request.getIdsList().stream().map(NotifyResultDataRequest.ResultIdentifier::getResultId).toList())
                                                    .build());
    responseObserver.onCompleted();
  }

  @Override
  public void createResultsMetaData(CreateResultsMetaDataRequest request, StreamObserver<CreateResultsMetaDataResponse> responseObserver) {
    if (blobMetadata == null) {
      blobMetadata = new BlobMetadata(request.getSessionId(), request.getCommunicationToken(), new ArrayList<>());
    }
    blobMetadata.names().addAll(request.getResultsList().stream().map(CreateResultsMetaDataRequest.ResultCreate::getName).toList());

    var metadata = request.getResultsList()
                          .stream()
                          .map(toResultCreateResultMetaData(request))
                          .toList();

    responseObserver.onNext(toCreateResultsMetaDataResponse(request, metadata));
    responseObserver.onCompleted();
  }

  @Override
  public void createResults(CreateResultsRequest request, StreamObserver<CreateResultsResponse> responseObserver) {
    if (uploadedBlobs == null) {
      uploadedBlobs = new UploadedBlobs(request.getSessionId(), request.getCommunicationToken(), new HashMap<>());
    }
    var blobs = request.getResultsList()
                       .stream()
                       .collect(toMap(CreateResultsRequest.ResultCreate::getName, resultCreate -> resultCreate.getData().toByteArray()));

    uploadedBlobs.blobs.putAll(blobs);

    var metadata = request.getResultsList()
                          .stream()
                          .map(toResultCreateResultMetaData(request))
                          .toList();
    responseObserver.onNext(toCreateResultsResponse(request, metadata));

    responseObserver.onCompleted();
  }

  private static CreateResultsResponse toCreateResultsResponse(CreateResultsRequest request, List<ResultMetaData> metadata) {
    return CreateResultsResponse.newBuilder()
                                .setCommunicationToken(request.getCommunicationToken())
                                .addAllResults(metadata)
                                .build();
  }

  private static Function<CreateResultsRequest.ResultCreate, ResultMetaData> toResultCreateResultMetaData(CreateResultsRequest request) {
    return resultCreate -> ResultMetaData.newBuilder()
                                         .setResultId(resultCreate.getName() + "-id")
                                         .setCreatedAt(Timestamp.newBuilder()
                                                                .setSeconds(Instant.now().getEpochSecond())
                                                                .build())
                                         .setSessionId(request.getSessionId())
                                         .setStatus(RESULT_STATUS_CREATED)
                                         .build();
  }

  private static CreateResultsMetaDataResponse toCreateResultsMetaDataResponse(CreateResultsMetaDataRequest request, List<ResultMetaData> metadata) {
    return CreateResultsMetaDataResponse.newBuilder()
                                        .setCommunicationToken(request.getCommunicationToken())
                                        .setCommunicationToken(request.getCommunicationToken())
                                        .addAllResults(metadata)
                                        .build();
  }

  private static Function<CreateResultsMetaDataRequest.ResultCreate, ResultMetaData> toResultCreateResultMetaData(CreateResultsMetaDataRequest request) {
    return resultCreate -> ResultMetaData.newBuilder()
                                         .setResultId(resultCreate.getName() + "-id")
                                         .setCreatedAt(Timestamp.newBuilder()
                                                                .setSeconds(Instant.now().getEpochSecond())
                                                                .build())
                                         .setSessionId(request.getSessionId())
                                         .setStatus(RESULT_STATUS_CREATED)
                                         .build();
  }


  public record UploadedBlobs(String sessionId, String communicationToken, Map<String, byte[]> blobs) {
  }

  public record BlobMetadata(String sessionId, String communicationToken, List<String> names) {
  }

  public record NotifyBlobs(String communicationToken, Map<String, String> ids) {
  }


}
