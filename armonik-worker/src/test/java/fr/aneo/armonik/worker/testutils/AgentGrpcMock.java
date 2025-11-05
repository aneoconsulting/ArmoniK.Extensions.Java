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
import fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.SubmitTasksResponse.TaskInfo;
import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static fr.aneo.armonik.api.grpc.v1.Objects.TaskOptions;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.*;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_CREATED;
import static java.util.stream.Collectors.toMap;

public class AgentGrpcMock extends AgentGrpc.AgentImplBase {

  public UploadedBlobs uploadedBlobs;
  public BlobMetadata blobMetadata;
  public NotifyBlobs notifyBlobs;
  public SubmittedTasks submittedTasks;

  private CompletableFuture<CreateResultsResponse> delayedCreateResults;
  private CompletableFuture<SubmitTasksResponse> delayedSubmitTasks;


  public void reset() {
    uploadedBlobs = null;
    blobMetadata = null;
    notifyBlobs = null;
    submittedTasks = null;
    delayedCreateResults = null;
    delayedSubmitTasks = null;
  }

  public void delayCreateResults(CompletableFuture<CreateResultsResponse> delayedResponse) {
    this.delayedCreateResults = delayedResponse;
  }

  public void delaySubmitTasks(CompletableFuture<SubmitTasksResponse> delayedResponse) {
    this.delayedSubmitTasks = delayedResponse;
  }

  @Override
  public void submitTasks(SubmitTasksRequest request, StreamObserver<SubmitTasksResponse> responseObserver) {
    captureSubmitTasksRequest(request);

    var response = buildSubmitTasksResponse(request);

    if (delayedSubmitTasks != null) {
      delayedSubmitTasks.whenComplete((customResponse, error) -> {
        if (error != null) {
          responseObserver.onError(error);
        } else {
          responseObserver.onNext(customResponse != null ? customResponse : response);
          responseObserver.onCompleted();
        }
      });
    } else {
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void notifyResultData(NotifyResultDataRequest request, StreamObserver<NotifyResultDataResponse> responseObserver) {
    captureNotifyResultDataRequest(request);

    var response = buildNotifyResultDataResponse(request);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void createResultsMetaData(CreateResultsMetaDataRequest request, StreamObserver<CreateResultsMetaDataResponse> responseObserver) {
    captureBlobMetadataRequest(request);

    var metadata = request.getResultsList()
                          .stream()
                          .map(toResultMetaData(request))
                          .toList();
    var response = buildCreateResultsMetaDataResponse(request, metadata);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void createResults(CreateResultsRequest request, StreamObserver<CreateResultsResponse> responseObserver) {
    captureCreateResultsRequest(request);

    var metadata = request.getResultsList()
                          .stream()
                          .map(toResultMetaData(request))
                          .toList();
    var response = buildCreateResultsResponse(request, metadata);

    if (delayedCreateResults != null) {
      delayedCreateResults.whenComplete((customResponse, error) -> {
        if (error != null) {
          responseObserver.onError(error);
        } else {
          responseObserver.onNext(customResponse != null ? customResponse : response);
          responseObserver.onCompleted();
        }
      });
    } else {
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  private void captureSubmitTasksRequest(SubmitTasksRequest request) {
    if (submittedTasks == null) {
      submittedTasks = new SubmittedTasks(
        request.getSessionId(),
        request.getCommunicationToken(),
        request.getTaskOptions(),
        new ArrayList<>()
      );
    }

    request.getTaskCreationsList()
           .forEach(taskCreation ->
             submittedTasks.taskCreations().add(new CapturedTaskCreation(
               taskCreation.getPayloadId(),
               taskCreation.getDataDependenciesList(),
               taskCreation.getExpectedOutputKeysList(),
               taskCreation.hasTaskOptions() ? taskCreation.getTaskOptions() : null
             )));
  }

  private void captureNotifyResultDataRequest(NotifyResultDataRequest request) {
    if (notifyBlobs == null) {
      notifyBlobs = new NotifyBlobs(request.getCommunicationToken(), new HashMap<>());
    }

    notifyBlobs.ids().putAll(request.getIdsList().stream().collect(toMap(
      NotifyResultDataRequest.ResultIdentifier::getSessionId,
      NotifyResultDataRequest.ResultIdentifier::getResultId
    )));
  }

  private void captureBlobMetadataRequest(CreateResultsMetaDataRequest request) {
    if (blobMetadata == null) {
      blobMetadata = new BlobMetadata(request.getSessionId(), request.getCommunicationToken(), new ArrayList<>());
    }
    blobMetadata.names().addAll(request.getResultsList()
                                       .stream()
                                       .map(CreateResultsMetaDataRequest.ResultCreate::getName)
                                       .toList()
    );
  }

  private void captureCreateResultsRequest(CreateResultsRequest request) {
    if (uploadedBlobs == null) {
      uploadedBlobs = new UploadedBlobs(request.getSessionId(), request.getCommunicationToken(), new HashMap<>());
    }

    var blobs = request.getResultsList()
                       .stream()
                       .collect(toMap(
                         CreateResultsRequest.ResultCreate::getName,
                         resultCreate -> resultCreate.getData().toByteArray()
                       ));
    uploadedBlobs.blobs.putAll(blobs);
  }

  private static SubmitTasksResponse buildSubmitTasksResponse(SubmitTasksRequest request) {
    var taskInfos = request.getTaskCreationsList()
                           .stream()
                           .map(taskCreation -> TaskInfo.newBuilder()
                                                        .setTaskId("task-" + taskCreation.getPayloadId())
                                                        .build())
                           .toList();

    return SubmitTasksResponse.newBuilder()
                              .addAllTaskInfos(taskInfos)
                              .build();
  }

  private static NotifyResultDataResponse buildNotifyResultDataResponse(NotifyResultDataRequest request) {
    return NotifyResultDataResponse.newBuilder()
                                   .addAllResultIds(
                                     request.getIdsList()
                                            .stream()
                                            .map(NotifyResultDataRequest.ResultIdentifier::getResultId)
                                            .toList()
                                   )
                                   .build();
  }

  private static CreateResultsMetaDataResponse buildCreateResultsMetaDataResponse(CreateResultsMetaDataRequest request, List<ResultMetaData> metadata) {
    return CreateResultsMetaDataResponse.newBuilder()
                                        .setCommunicationToken(request.getCommunicationToken())
                                        .addAllResults(metadata)
                                        .build();
  }

  private static CreateResultsResponse buildCreateResultsResponse(CreateResultsRequest request, List<ResultMetaData> metadata) {
    return CreateResultsResponse.newBuilder()
                                .setCommunicationToken(request.getCommunicationToken())
                                .addAllResults(metadata)
                                .build();
  }

  private static Function<CreateResultsMetaDataRequest.ResultCreate, ResultMetaData> toResultMetaData(CreateResultsMetaDataRequest request) {
    return resultCreate -> ResultMetaData.newBuilder()
                                         .setResultId(resultCreate.getName() + "-id")
                                         .setCreatedAt(Timestamp.newBuilder()
                                                                .setSeconds(Instant.now().getEpochSecond())
                                                                .build())
                                         .setSessionId(request.getSessionId())
                                         .setStatus(RESULT_STATUS_CREATED)
                                         .build();
  }

  private static Function<CreateResultsRequest.ResultCreate, ResultMetaData> toResultMetaData(CreateResultsRequest request) {
    return resultCreate -> ResultMetaData.newBuilder()
                                         .setResultId(resultCreate.getName() + "-id")
                                         .setCreatedAt(Timestamp.newBuilder()
                                                                .setSeconds(Instant.now().getEpochSecond())
                                                                .build())
                                         .setSessionId(request.getSessionId())
                                         .setStatus(RESULT_STATUS_CREATED)
                                         .build();
  }

  public record UploadedBlobs(String sessionId,
                              String communicationToken,
                              Map<String, byte[]> blobs) {
  }


  public record BlobMetadata(String sessionId,
                             String communicationToken,
                             List<String> names) {
  }


  public record NotifyBlobs(String communicationToken,
                            Map<String, String> ids) {
  }

  public record SubmittedTasks(String sessionId,
                               String communicationToken,
                               TaskOptions defaultTaskOptions,
                               List<CapturedTaskCreation> taskCreations) {
  }

  public record CapturedTaskCreation(String payloadId,
                                     List<String> dataDependencies,
                                     List<String> expectedOutputKeys,
                                     TaskOptions taskOptions) {
  }
}
