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

import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.migrationsupport.rules.ExternalResourceSupport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataRequest;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.UploadResultDataResponse;
import static fr.aneo.armonik.client.blob.BlobHandleTestFactory.blobHandle;
import static fr.aneo.armonik.client.blob.DefaultBlobService.*;
import static org.assertj.core.api.Assertions.assertThat;

class DefaultBlobServiceTest {

  @RegisterExtension
  public static final ExternalResourceSupport externalResourceSupport = new ExternalResourceSupport();

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private DefaultBlobService blobService;
  private ResultsServiceMock resultsServiceMock;

  @BeforeEach
  void setUp() throws IOException {
    resultsServiceMock = new ResultsServiceMock();
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                                               .directExecutor()
                                               .addService(resultsServiceMock)
                                               .build()
                                               .start());

    var channel = grpcCleanup.register(InProcessChannelBuilder.forName(serverName)
                                                              .directExecutor()
                                                              .build());
    blobService = new DefaultBlobService(channel);
  }

  @Test
  void should_successfully_upload_blob() {
    // Given
    var blobHandle = blobHandle();

    // When
    blobService.uploadBlobData(blobHandle, BlobDefinition.from("Hello".getBytes())).toCompletableFuture().join();

    // Then
    assertThat(resultsServiceMock.sessionId).isEqualTo(blobHandle.sessionHandle().id().toString());
    assertThat(resultsServiceMock.blobId).isEqualTo(blobHandle.metadata().toCompletableFuture().join().id().toString());
    assertThat(resultsServiceMock.receivedData.toByteArray()).asString().isEqualTo("Hello");
  }

  @Test
  void should_send_data_in_chunks() {
    // Given
    var blobHandle = blobHandle();
    final int REMAINDER = 123;
    byte[] payload = new byte[UPLOAD_CHUNK_SIZE * 2 + REMAINDER];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 251);
    }

    // When
    blobService.uploadBlobData(blobHandle, BlobDefinition.from(payload)).toCompletableFuture().join();

    // Then
    assertThat(resultsServiceMock.sessionId).isEqualTo(blobHandle.sessionHandle().id().toString());
    assertThat(resultsServiceMock.blobId).isEqualTo(blobHandle.metadata().toCompletableFuture().join().id().toString());
    assertThat(resultsServiceMock.dataChunkSizes).containsExactly(UPLOAD_CHUNK_SIZE, UPLOAD_CHUNK_SIZE, REMAINDER);
    assertThat(resultsServiceMock.receivedData.toByteArray()).isEqualTo(payload);
  }

  static class ResultsServiceMock extends ResultsGrpc.ResultsImplBase {
    boolean firstCall = true;
    String sessionId;
    String blobId;
    ByteArrayOutputStream receivedData = new ByteArrayOutputStream();
    List<Integer> dataChunkSizes = new ArrayList<>();

    @Override
    public StreamObserver<UploadResultDataRequest> uploadResultData(
      StreamObserver<UploadResultDataResponse> responseObserver) {
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
}
