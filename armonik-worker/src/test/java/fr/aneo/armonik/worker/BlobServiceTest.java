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
import fr.aneo.armonik.worker.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.definition.blob.OutputBlobDefinition;
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

    var inputs = Map.of("input1", InputBlobDefinition.from("blobName1", data1));

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

    var inputs = Map.of("input1", InputBlobDefinition.from("blobName1", data1));

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
    var inputs = Map.of("input1", InputBlobDefinition.from("blobName1", new ByteArrayInputStream("Hello John !".getBytes())));

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
  @DisplayName("should prepare output blob metadata without data")
  void should_prepare_output_blob_metadata_without_data() {
    // Given
    var outputs = Map.of(
      "output1", OutputBlobDefinition.from("resultName1"),
      "output2", OutputBlobDefinition.create()
    );

    // When
    var outputHandles = blobService.prepareBlobs(outputs);

    // Then
    assertThat(outputHandles).hasSize(2);
    assertThat(outputHandles).containsOnlyKeys("output1", "output2");

    var output1Handle = outputHandles.get("output1");
    assertThat(output1Handle.name()).isEqualTo("resultName1");
    assertThat(output1Handle.sessionId().asString()).isEqualTo("sessionId");

    var output2Handle = outputHandles.get("output2");
    assertThat(output2Handle.name()).isEmpty();
    assertThat(output2Handle.sessionId().asString()).isEqualTo("sessionId");

    var blobInfo1 = output1Handle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(blobInfo1.id()).isNotNull();
    assertThat(blobInfo1.status()).isNotNull();
    assertThat(blobInfo1.creationDate()).isNotNull();

    var blobInfo2 = output2Handle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(blobInfo2.id()).isNotNull();
    assertThat(blobInfo2.status()).isNotNull();
    assertThat(blobInfo2.creationDate()).isNotNull();

    assertThat(agentGrpcMock.uploadedBlobs).isNull();
    assertThat(agentGrpcMock.notifyBlobs).isNull();
    assertThat(agentGrpcMock.blobMetadata.sessionId()).isEqualTo("sessionId");
    assertThat(agentGrpcMock.blobMetadata.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.blobMetadata.names()).containsExactlyInAnyOrder("resultName1", "");

    assertThat(tempDir.resolve(blobInfo1.id().asString())).doesNotExist();
    assertThat(tempDir.resolve(blobInfo2.id().asString())).doesNotExist();
  }

  @Test
  @DisplayName("should prepare mixed blob metadata for both inputs and outputs")
  void should_prepare_mixed_blob_metadata_for_both_inputs_and_outputs() {
    // Given
    var mixed = Map.of(
      "input1", InputBlobDefinition.from("inputName1", new byte[100]),
      "output1", OutputBlobDefinition.from("outputName1")
    );

    // When
    var handles = blobService.prepareBlobs(mixed);

    // Then
    assertThat(handles).hasSize(2);
    assertThat(handles).containsOnlyKeys("input1", "output1");

    var inputHandle = handles.get("input1");
    assertThat(inputHandle.name()).isEqualTo("inputName1");

    var outputHandle = handles.get("output1");
    assertThat(outputHandle.name()).isEqualTo("outputName1");

    var inputBlobInfo = inputHandle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(inputBlobInfo.id()).isNotNull();

    var outputBlobInfo = outputHandle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(outputBlobInfo.id()).isNotNull();

    assertThat(agentGrpcMock.uploadedBlobs).isNull();
    assertThat(agentGrpcMock.notifyBlobs).isNull();
    assertThat(agentGrpcMock.blobMetadata.sessionId()).isEqualTo("sessionId");
    assertThat(agentGrpcMock.blobMetadata.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.blobMetadata.names()).containsExactlyInAnyOrder("inputName1", "outputName1");

    assertThat(tempDir.resolve(inputBlobInfo.id().asString())).doesNotExist();
    assertThat(tempDir.resolve(outputBlobInfo.id().asString())).doesNotExist();
  }
}
