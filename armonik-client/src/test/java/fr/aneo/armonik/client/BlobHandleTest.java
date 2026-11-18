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
package fr.aneo.armonik.client;

import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataRequest;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataResponse;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.client.definition.blob.InMemoryBlobData;
import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.ResultsGrpcMock;
import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static fr.aneo.armonik.client.BlobHandle.UPLOAD_CHUNK_SIZE;
import static fr.aneo.armonik.client.TestDataFactory.*;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class BlobHandleTest extends InProcessGrpcTestBase {

  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private BlobHandle blobHandle;

  @BeforeEach
  void setUp() {
    blobHandle = new BlobHandle(sessionId(), completedFuture(blobInfo("BlobId")), channelPool);
  }

  @Test
  void should_successfully_upload_blob_data() throws InterruptedException, TimeoutException {
    // Given
    var blobData = InMemoryBlobData.from("Hello".getBytes());

    // When
    blobHandle.uploadData(blobData).toCompletableFuture().join();
    resultsGrpcMock.awaitUploadsComplete(2, SECONDS);

    // Then
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(1);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).sessionId).isEqualTo(blobHandle.sessionId().asString());
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).blobId).isEqualTo("BlobId");
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).receivedData.toByteArray()).asString().isEqualTo("Hello");
  }

  @Test
  void should_send_blob_data_in_chunks() {
    // Given
    final int REMAINDER = 123;
    byte[] payload = new byte[UPLOAD_CHUNK_SIZE * 2 + REMAINDER];
    IntStream.range(0, payload.length).forEach(i -> payload[i] = (byte) (i % 251));

    // When
    blobHandle.uploadData(InMemoryBlobData.from(payload)).toCompletableFuture().join();

    // Then
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(1);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).sessionId).isEqualTo(blobHandle.sessionId().asString());
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).blobId).isEqualTo("BlobId");
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).dataChunkSizes).containsExactly(UPLOAD_CHUNK_SIZE, UPLOAD_CHUNK_SIZE, REMAINDER);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).receivedData.toByteArray()).isEqualTo(payload);
  }

  @Test
  void should_send_the_same_bytes_when_the_channel_retries_an_upload() throws Exception {
    // Given: a server that fails the first upload attempt once it has received every chunk, and a
    // channel that retries it, as the channels of the client do
    var attempts = new CopyOnWriteArrayList<byte[]>();
    var name = InProcessServerBuilder.generateName();
    var server = InProcessServerBuilder.forName(name).directExecutor().addService(new ResultsGrpc.ResultsImplBase() {
      @Override
      public StreamObserver<UploadResultDataRequest> uploadResultData(StreamObserver<UploadResultDataResponse> responseObserver) {
        var received = new ByteArrayOutputStream();
        return new StreamObserver<>() {
          @Override
          public void onNext(UploadResultDataRequest request) {
            if (request.hasDataChunk()) received.writeBytes(request.getDataChunk().toByteArray());
          }

          @Override
          public void onError(Throwable throwable) {
          }

          @Override
          public void onCompleted() {
            attempts.add(received.toByteArray());
            if (attempts.size() == 1) {
              responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
            } else {
              responseObserver.onNext(UploadResultDataResponse.getDefaultInstance());
              responseObserver.onCompleted();
            }
          }
        };
      }
    }).build().start();
    var retryPolicy = Map.of("maxAttempts", 2.0, "initialBackoff", "0.01s", "maxBackoff", "0.01s", "backoffMultiplier", 1.0,
      "retryableStatusCodes", List.of("UNAVAILABLE"));
    var serviceConfig = Map.of("methodConfig", List.of(Map.of("name", List.of(Map.of()), "retryPolicy", retryPolicy)));
    var retryingPool = ManagedChannelPool.create(1, new RetryPolicy(1, Duration.ofMillis(10), Duration.ofMillis(10), 1),
      () -> InProcessChannelBuilder.forName(name).directExecutor().defaultServiceConfig(serviceConfig).enableRetry().build());
    // Each chunk holds its own byte value, so that a chunk replayed with the bytes of another shows
    var payload = new byte[UPLOAD_CHUNK_SIZE * 3 + 7];
    IntStream.range(0, payload.length).forEach(i -> payload[i] = (byte) (i / UPLOAD_CHUNK_SIZE + 1));
    var handle = new BlobHandle(sessionId(), completedFuture(blobInfo("BlobId")), retryingPool);

    try {
      // When
      handle.uploadData(InMemoryBlobData.from(payload)).toCompletableFuture().get(10, SECONDS);

      // Then: the retried attempt carried the same bytes as the first one
      assertThat(attempts).hasSize(2);
      assertThat(attempts.get(0)).isEqualTo(payload);
      assertThat(attempts.get(1)).isEqualTo(payload);
    } finally {
      retryingPool.close();
      server.shutdownNow();
    }
  }

  @Test
  void should_successfully_download_blob_data() {
    // Given
    resultsGrpcMock.setDownloadContentFor(blobId("BlobId"), "Hello World!".getBytes());

    // When
    var content = blobHandle.downloadData().toCompletableFuture().join();

    // Then
    assertThat(content).isEqualTo("Hello World!".getBytes());
  }

  @Override
  protected List<BindableService> services() {
    return List.of(resultsGrpcMock);
  }
}
