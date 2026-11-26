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
import fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.client.BlobId;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.protobuf.ByteString.copyFrom;
import static fr.aneo.armonik.client.TestDataFactory.blobId;

public class ResultsGrpcMock extends ResultsGrpc.ResultsImplBase {
  private BlobId downloadFailedId;
  private BlobId downloadSuccessId;
  private byte[] downloadContent;
  public List<UploadedDataInfo> uploadedDataInfos = new ArrayList<>();
  public List<MetadataRequest> metadataRequests = new ArrayList<>();
  public int metadataCalls;
  public volatile RuntimeException metadataError;
  public volatile byte[] failingUploadData;
  /** Delay before an upload whose data is failingUploadData is answered with its error. */
  public volatile long failingUploadDelayMillis;
  public final AtomicInteger downloadCalls = new AtomicInteger();
  private final List<CompletableFuture<UploadedDataInfo>> pendingUploads = new CopyOnWriteArrayList<>();
  /** DataChunkMaxSize returned by GetServiceConfiguration; 0 leaves the call unimplemented, as an old control plane would. */
  public volatile int dataChunkMaxSize;
  public final AtomicInteger serviceConfigurationCalls = new AtomicInteger();
  /** When set, GetServiceConfiguration fails with this error. */
  public volatile RuntimeException serviceConfigurationError;
  public final List<CreateResultsRequest> createResultsRequests = new CopyOnWriteArrayList<>();
  /** Data received through CreateResults, by result id. */
  public final Map<String, byte[]> createdData = new ConcurrentHashMap<>();
  /** Number of next CreateResults calls that fail with UNAVAILABLE. */
  public final AtomicInteger failingCreateResults = new AtomicInteger();
  /** When set, a CreateResults call holding a result of this name fails with INVALID_ARGUMENT. */
  public volatile String rejectedCreateResultsName;
  /** Status of each result, as the event streams report it; results not set are CREATED. */
  public final Map<String, ResultStatusOuterClass.ResultStatus> statuses = new ConcurrentHashMap<>();
  /** Size ListResults reports for each result; results not set report the size of their download content. */
  public final Map<String, Long> sizes = new ConcurrentHashMap<>();
  /** Every ListResults request received, in order. */
  public final List<ListResultsRequest> listResultsRequests = new CopyOnWriteArrayList<>();
  /** Number of the next ListResults calls that fail with {@link #failingListResultsStatus}. */
  public final AtomicInteger failingListResults = new AtomicInteger();
  /** Status of the failing ListResults calls; UNAVAILABLE by default. */
  public volatile io.grpc.Status failingListResultsStatus = io.grpc.Status.UNAVAILABLE;
  /** Results ListResults omits, as if their record had been removed. */
  public final java.util.Set<String> missingResults = ConcurrentHashMap.newKeySet();
  /** Notified of every status set through {@link #setStatus}; kept across {@link #reset()}. */
  private final List<java.util.function.BiConsumer<String, ResultStatusOuterClass.ResultStatus>> statusListeners = new CopyOnWriteArrayList<>();


  public void failDownloadFor(BlobId id) {
    this.downloadFailedId = id;
  }

  public void addStatusListener(java.util.function.BiConsumer<String, ResultStatusOuterClass.ResultStatus> statusListener) {
    statusListeners.add(statusListener);
  }

  public void setStatus(BlobId id, ResultStatusOuterClass.ResultStatus status) {
    statuses.put(id.asString(), status);
    statusListeners.forEach(statusListener -> statusListener.accept(id.asString(), status));
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
    this.metadataCalls = 0;
    this.metadataError = null;
    this.failingUploadData = null;
    this.failingUploadDelayMillis = 0;
    this.downloadCalls.set(0);
    this.pendingUploads.clear();
    this.dataChunkMaxSize = 0;
    this.serviceConfigurationCalls.set(0);
    this.serviceConfigurationError = null;
    this.createResultsRequests.clear();
    this.createdData.clear();
    this.failingCreateResults.set(0);
    this.rejectedCreateResultsName = null;
    this.statuses.clear();
    this.sizes.clear();
    this.listResultsRequests.clear();
    this.failingListResults.set(0);
    this.failingListResultsStatus = io.grpc.Status.UNAVAILABLE;
    this.missingResults.clear();
  }

  /** Lists the results whose ids the filter names, as the watcher asks for their sizes. */
  @Override
  public void listResults(ListResultsRequest request, StreamObserver<ListResultsResponse> responseObserver) {
    listResultsRequests.add(request);
    if (failingListResults.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
      responseObserver.onError(failingListResultsStatus.asRuntimeException());
      return;
    }
    var response = ListResultsResponse.newBuilder();
    for (var and : request.getFilters().getOrList()) {
      for (var field : and.getAndList()) {
        if (!field.hasFilterString()) continue;
        var id = field.getFilterString().getValue();
        if (missingResults.contains(id)) continue;
        var status = statuses.getOrDefault(id, ResultStatusOuterClass.ResultStatus.RESULT_STATUS_CREATED);
        var size = sizes.getOrDefault(id, (long) contentFor(blobId(id)).length);
        response.addResults(ResultRaw.newBuilder().setResultId(id).setStatus(status).setSize(size));
      }
    }
    response.setTotal(response.getResultsCount());
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  private byte[] contentFor(BlobId id) {
    return ((downloadSuccessId != null) && downloadSuccessId.equals(id))
      ? downloadContent
      : ("data-" + id).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void getServiceConfiguration(fr.aneo.armonik.api.grpc.v1.Objects.Empty request, StreamObserver<ResultsServiceConfigurationResponse> responseObserver) {
    serviceConfigurationCalls.incrementAndGet();
    if (serviceConfigurationError != null) {
      responseObserver.onError(serviceConfigurationError);
      return;
    }
    if (dataChunkMaxSize <= 0) {
      super.getServiceConfiguration(request, responseObserver);
      return;
    }
    responseObserver.onNext(ResultsServiceConfigurationResponse.newBuilder().setDataChunkMaxSize(dataChunkMaxSize).build());
    responseObserver.onCompleted();
  }

  @Override
  public void createResults(CreateResultsRequest request, StreamObserver<CreateResultsResponse> responseObserver) {
    if (failingCreateResults.getAndUpdate(remaining -> Math.max(remaining - 1, 0)) > 0) {
      responseObserver.onError(io.grpc.Status.UNAVAILABLE.withDescription("transient failure").asRuntimeException());
      return;
    }
    if (rejectedCreateResultsName != null && request.getResultsList().stream().anyMatch(result -> result.getName().equals(rejectedCreateResultsName))) {
      responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT.withDescription("rejected result " + rejectedCreateResultsName).asRuntimeException());
      return;
    }
    if (request.getSerializedSize() > dataChunkMaxSize) {
      responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT.withDescription("request larger than DataChunkMaxSize").asRuntimeException());
      return;
    }
    createResultsRequests.add(request);
    var builder = CreateResultsResponse.newBuilder();
    request.getResultsList().forEach(result -> {
      var id = UUID.randomUUID().toString();
      createdData.put(id, result.getData().toByteArray());
      builder.addResults(ResultRaw.newBuilder()
                                  .setResultId(id)
                                  .setName(result.getName())
                                  .setManualDeletion(result.getManualDeletion())
                                  .setCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                                  .build());
    });
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void createResultsMetaData(CreateResultsMetaDataRequest request, StreamObserver<CreateResultsMetaDataResponse> responseObserver) {
    var builder = CreateResultsMetaDataResponse.newBuilder();
    metadataCalls++;
    if (metadataError != null) {
      responseObserver.onError(metadataError);
      return;
    }
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
    downloadCalls.incrementAndGet();
    var id = blobId(request.getResultId());
    if (downloadFailedId != null && downloadFailedId.equals(blobId(request.getResultId()))) {
      responseObserver.onError(new RuntimeException("Failed to download result"));
      return;
    }

    byte[] bytes = contentFor(id);
    responseObserver.onNext(DownloadResultDataResponse.newBuilder()
                                                      .setDataChunk(copyFrom(bytes))
                                                      .build());
    responseObserver.onCompleted();
  }


  @Override
  public StreamObserver<UploadResultDataRequest> uploadResultData(StreamObserver<UploadResultDataResponse> responseObserver) {
    var uploadCompletion = new CompletableFuture<UploadedDataInfo>();
    pendingUploads.add(uploadCompletion);

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
        pendingUploads.remove(uploadCompletion);
        uploadCompletion.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
        if (failingUploadData != null && Arrays.equals(failingUploadData, uploadedDataInfo.receivedData.toByteArray())) {
          if (failingUploadDelayMillis > 0) {
            try {
              Thread.sleep(failingUploadDelayMillis);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          pendingUploads.remove(uploadCompletion);
          responseObserver.onError(io.grpc.Status.INTERNAL.withDescription("Upload failed").asRuntimeException());
          uploadCompletion.completeExceptionally(new IllegalStateException("Upload failed"));
          return;
        }
        responseObserver.onNext(UploadResultDataResponse.getDefaultInstance());
        responseObserver.onCompleted();
        uploadedDataInfos.add(uploadedDataInfo);
        pendingUploads.remove(uploadCompletion);
        uploadCompletion.complete(uploadedDataInfo);
      }
    };
  }

  public void awaitUploadsComplete(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    while (pendingUploads.isEmpty() && uploadedDataInfos.isEmpty()) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        return;
      }
      Thread.sleep(10);  // Poll every 10ms
    }

    // Now wait for all pending uploads to complete
    var snapshot = List.copyOf(pendingUploads);
    for (var future : snapshot) {
      var remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        throw new TimeoutException("Timeout waiting for uploads to complete. Remaining: " + pendingUploads.size());
      }
      try {
        future.get(remainingNanos, TimeUnit.NANOSECONDS);
      } catch (ExecutionException e) {
        // Upload failed, but we still count it as "complete"
      }
    }
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
