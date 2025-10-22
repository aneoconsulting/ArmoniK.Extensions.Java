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
package fr.aneo.armonik.worker;

import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import fr.aneo.armonik.worker.definition.blob.BlobDefinition;
import fr.aneo.armonik.worker.testutils.AgentGrpcMock;
import fr.aneo.armonik.worker.testutils.InProcessGrpcTestBase;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static fr.aneo.armonik.worker.BlobService.MAX_UPLOAD_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

class BlobServiceTest extends InProcessGrpcTestBase {

  private final AgentGrpcMock agentGrpcMock = new AgentGrpcMock();
  private BlobService blobService;

  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() {
    var agentStub = AgentGrpc.newFutureStub(channel);
    var agentNotifier = new AgentNotifier(agentStub, SessionId.from("sessionId"), "communicationToken");
    var blobFileWriter = new BlobFileWriter(tempDir, agentNotifier);
    blobService = new BlobService(agentStub, blobFileWriter, "communicationToken", SessionId.from("sessionId"));
    agentGrpcMock.reset();
  }

  @Override
  protected List<BindableService> services() {
    return List.of(agentGrpcMock);
  }

  @Test
  @DisplayName("should upload blob data when size is less or equal MAX_UPLOAD_SIZE")
  void should_upload_blob_data_when_size_is_less_or_equal_4MB() {
    // Given
    byte[] data1 = new byte[MAX_UPLOAD_SIZE];
    IntStream.range(0, data1.length).forEach(i -> data1[i] = (byte) (i % 251));

    var inputs = Map.of("input1", BlobDefinition.from("blobName1", data1));

    // When
    blobService.createBlobs(inputs);

    // Then
    assertThat(agentGrpcMock.uploadedBlobs.sessionId()).isEqualTo("sessionId");
    assertThat(agentGrpcMock.uploadedBlobs.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.uploadedBlobs.blobs()).containsExactly(Map.entry("blobName1", data1));
  }

  @Test
  @DisplayName("should write blob data in a file when size is greater than MAX_UPLOAD_SIZE")
  void should_write_blob_data_in_a_file() {
    // Given
    byte[] data1 = new byte[MAX_UPLOAD_SIZE + 1];
    IntStream.range(0, data1.length).forEach(i -> data1[i] = (byte) (i % 251));

    var inputs = Map.of("input1", BlobDefinition.from("blobName1", data1));

    // When
    var inputHandles = blobService.createBlobs(inputs);

    // Then
    assertThat(inputHandles).hasSize(1);
    assertThat(inputHandles).containsOnlyKeys("input1");

    var blobHandle = inputHandles.get("input1");
    assertThat(blobHandle.name()).isEqualTo("blobName1");
    assertThat(blobHandle.sessionId().asString()).isEqualTo("sessionId");

    var blobInfo = blobHandle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(blobInfo.id()).isNotNull();
    assertThat(blobInfo.status()).isNotNull();
    assertThat(blobInfo.creationDate()).isNotNull();

    assertThat(agentGrpcMock.uploadedBlobs).isNull();
    assertThat(agentGrpcMock.blobMetadata.sessionId()).isEqualTo(blobHandle.sessionId().asString());
    assertThat(agentGrpcMock.blobMetadata.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.blobMetadata.names()).containsExactly(blobHandle.name());
    assertThat(agentGrpcMock.notifyBlobs.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.notifyBlobs.ids()).containsExactly(Map.entry(blobHandle.sessionId().asString(), blobInfo.id().asString()));

    assertThat(tempDir.resolve(blobInfo.id().asString())).exists().isRegularFile();
  }

  @Test
  @DisplayName("should write blob data in a file for StreamBlob")
  void should_write_blob_data_in_a_file_for_StreamBlob() {
    // Given
    var inputs = Map.of("input1", BlobDefinition.from("blobName1", new ByteArrayInputStream("Hello John !".getBytes())));

    // When
    var inputHandles = blobService.createBlobs(inputs);

    // Then
    assertThat(inputHandles).hasSize(1);
    assertThat(inputHandles).containsOnlyKeys("input1");

    var blobHandle = inputHandles.get("input1");
    assertThat(blobHandle.name()).isEqualTo("blobName1");
    assertThat(blobHandle.sessionId().asString()).isEqualTo("sessionId");

    var blobInfo = blobHandle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(blobInfo.id()).isNotNull();
    assertThat(blobInfo.status()).isNotNull();
    assertThat(blobInfo.creationDate()).isNotNull();

    assertThat(agentGrpcMock.uploadedBlobs).isNull();
    assertThat(agentGrpcMock.blobMetadata.sessionId()).isEqualTo(blobHandle.sessionId().asString());
    assertThat(agentGrpcMock.blobMetadata.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.blobMetadata.names()).containsExactly(blobHandle.name());
    assertThat(agentGrpcMock.notifyBlobs.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.notifyBlobs.ids()).containsExactly(Map.entry(blobHandle.sessionId().asString(), blobInfo.id().asString()));

    assertThat(tempDir.resolve(blobInfo.id().asString())).exists().isRegularFile();
  }
}
