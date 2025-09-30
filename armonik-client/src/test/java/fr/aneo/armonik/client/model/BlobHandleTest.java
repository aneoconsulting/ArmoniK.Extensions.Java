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
package fr.aneo.armonik.client.model;

import fr.aneo.armonik.client.definition.BlobDefinition;
import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.ResultsGrpcMock;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static fr.aneo.armonik.client.model.BlobHandle.UPLOAD_CHUNK_SIZE;
import static fr.aneo.armonik.client.model.TestDataFactory.*;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

class BlobHandleTest extends InProcessGrpcTestBase {

  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private BlobHandle blobHandle;

  @BeforeEach
  void setUp() {
    blobHandle = new BlobHandle(sessionId(), completedFuture(blobInfo("BlobId")), channel);
  }

  @Test
  void should_successfully_upload_blob_data() {
    // Given
    var blobDefinition = BlobDefinition.from("Hello".getBytes());

    // When
    blobHandle.uploadData(blobDefinition).toCompletableFuture().join();

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
    blobHandle.uploadData(BlobDefinition.from(payload)).toCompletableFuture().join();

    // Then
    assertThat(resultsGrpcMock.uploadedDataInfos).hasSize(1);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).sessionId).isEqualTo(blobHandle.sessionId().asString());
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).blobId).isEqualTo("BlobId");
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).dataChunkSizes).containsExactly(UPLOAD_CHUNK_SIZE, UPLOAD_CHUNK_SIZE, REMAINDER);
    assertThat(resultsGrpcMock.uploadedDataInfos.get(0).receivedData.toByteArray()).isEqualTo(payload);
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
