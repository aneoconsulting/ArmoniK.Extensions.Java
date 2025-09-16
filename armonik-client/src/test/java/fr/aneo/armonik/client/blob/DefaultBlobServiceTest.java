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

import fr.aneo.armonik.client.mocks.ResultsGrpcMock;
import fr.aneo.armonik.client.task.TaskDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.migrationsupport.rules.ExternalResourceSupport;

import java.io.IOException;
import java.util.stream.IntStream;

import static fr.aneo.armonik.client.blob.BlobHandleFixture.blobHandle;
import static fr.aneo.armonik.client.blob.DefaultBlobService.UPLOAD_CHUNK_SIZE;
import static fr.aneo.armonik.client.session.SessionHandleFixture.sessionHandle;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class DefaultBlobServiceTest {

  @RegisterExtension
  public static final ExternalResourceSupport externalResourceSupport = new ExternalResourceSupport();

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private DefaultBlobService blobService;
  private ResultsGrpcMock resultsGrpcMock;

  @BeforeEach
  void setUp() throws IOException {
    resultsGrpcMock = new ResultsGrpcMock();
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                                               .directExecutor()
                                               .addService(resultsGrpcMock)
                                               .build()
                                               .start());

    var channel = grpcCleanup.register(InProcessChannelBuilder.forName(serverName)
                                                              .directExecutor()
                                                              .build());
    blobService = new DefaultBlobService(channel);
  }

  @Test
  void should_allocate_blob_handle_successfully() {
    // Given
    var session = sessionHandle();
    var taskDefinition = new TaskDefinition().withInput("name", BlobDefinition.from("John".getBytes(UTF_8)))
                                             .withInput("age", BlobDefinition.from("42".getBytes(UTF_8)))
                                             .withInput("address", blobHandle(session))
                                             .withOutput("result");

    // When
    var blobHandlesAllocation = blobService.allocateBlobHandles(session, taskDefinition);

    // Then
    assertThat(blobHandlesAllocation.payloadHandle().sessionHandle()).isEqualTo(session);
    assertThat(blobHandlesAllocation.payloadHandle().metadata().toCompletableFuture().join().id()).isNotNull();

    assertThat(blobHandlesAllocation.inputHandlesByName()).hasSize(2).containsOnlyKeys("name", "age");
    assertThat(blobHandlesAllocation.inputHandlesByName().values()).allSatisfy( blobHandle -> {
      assertThat(blobHandle.sessionHandle()).isEqualTo(session);
      assertThat(blobHandle.metadata().toCompletableFuture().join().id()).isNotNull();
    });

    assertThat(blobHandlesAllocation.outputHandlesByName()).hasSize(1).containsOnlyKeys("result");
    assertThat(blobHandlesAllocation.outputHandlesByName().values()).allSatisfy( blobHandle -> {
      assertThat(blobHandle.sessionHandle()).isEqualTo(session);
      assertThat(blobHandle.metadata().toCompletableFuture().join().id()).isNotNull();
    });
  }

  @Test
  void should_successfully_upload_blob() {
    // Given
    var blobHandle = blobHandle(sessionHandle());

    // When
    blobService.uploadBlobData(blobHandle, BlobDefinition.from("Hello".getBytes())).toCompletableFuture().join();

    // Then
    assertThat(resultsGrpcMock.sessionId).isEqualTo(blobHandle.sessionHandle().id().toString());
    assertThat(resultsGrpcMock.blobId).isEqualTo(blobHandle.metadata().toCompletableFuture().join().id().toString());
    assertThat(resultsGrpcMock.receivedData.toByteArray()).asString().isEqualTo("Hello");
  }

  @Test
  void should_send_data_in_chunks() {
    // Given
    var blobHandle = blobHandle();
    final int REMAINDER = 123;
    byte[] payload = new byte[UPLOAD_CHUNK_SIZE * 2 + REMAINDER];
    IntStream.range(0, REMAINDER)
             .forEach(i -> payload[i] = (byte) (i % 251));

    // When
    blobService.uploadBlobData(blobHandle, BlobDefinition.from(payload)).toCompletableFuture().join();

    // Then
    assertThat(resultsGrpcMock.sessionId).isEqualTo(blobHandle.sessionHandle().id().toString());
    assertThat(resultsGrpcMock.blobId).isEqualTo(blobHandle.metadata().toCompletableFuture().join().id().toString());
    assertThat(resultsGrpcMock.dataChunkSizes).containsExactly(UPLOAD_CHUNK_SIZE, UPLOAD_CHUNK_SIZE, REMAINDER);
    assertThat(resultsGrpcMock.receivedData.toByteArray()).isEqualTo(payload);
  }

}
