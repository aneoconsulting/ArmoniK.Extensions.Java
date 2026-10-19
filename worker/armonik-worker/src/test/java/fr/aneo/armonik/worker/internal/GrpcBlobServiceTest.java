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
package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import fr.aneo.armonik.worker.domain.SessionId;
import fr.aneo.armonik.worker.domain.definition.blob.OutputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.testutils.AgentGrpcMock;
import fr.aneo.armonik.worker.testutils.InProcessGrpcTestBase;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static fr.aneo.armonik.worker.internal.GrpcBlobService.MAX_BLOBS_PER_REQUEST;
import static fr.aneo.armonik.worker.internal.AgentRequests.MAX_REQUEST_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

class GrpcBlobServiceTest extends InProcessGrpcTestBase {

  private final AgentGrpcMock agentGrpcMock = new AgentGrpcMock();
  private GrpcBlobService blobService;

  @TempDir
  Path tempDir;
  private SessionId sessionId;

  @BeforeEach
  void setUp() {
    sessionId = SessionId.from("sessionId");
    var agentStub = AgentGrpc.newFutureStub(channel);
    var agentNotifier = new AgentNotifier(agentStub, sessionId, "communicationToken");
    var blobFileWriter = new BlobFileWriter(tempDir, agentNotifier);
    blobService = new GrpcBlobService(agentStub, blobFileWriter, SessionId.from("sessionId"), "communicationToken");
    agentGrpcMock.reset();
  }

  @Override
  protected List<BindableService> services() {
    return List.of(agentGrpcMock);
  }

  @Test
  @DisplayName("should upload blob data inline when it fits in one Agent request")
  void should_upload_blob_data_when_it_fits_in_one_request() {
    // Given
    byte[] data1 = new byte[MAX_REQUEST_SIZE - 1024];
    IntStream.range(0, data1.length).forEach(i -> data1[i] = (byte) (i % 251));

    var inputs = Map.of("input1", InputBlobDefinition.from("blobName1", data1));

    // When
    blobService.createBlobs(inputs);

    // Then
    assertThat(agentGrpcMock.uploadedBlobs.sessionId()).isEqualTo("sessionId");
    assertThat(agentGrpcMock.uploadedBlobs.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.uploadedBlobs.blobs()).containsExactly(Map.entry("blobName1", data1));
    assertThat(agentGrpcMock.requestSizes).allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(MAX_REQUEST_SIZE));
  }

  @Test
  @DisplayName("should write blob data in a file when the request would exceed the Agent limit")
  void should_write_blob_data_in_a_file() {
    // Given: the data alone is below the limit, but not with the request envelope
    byte[] data1 = new byte[MAX_REQUEST_SIZE - 16];
    IntStream.range(0, data1.length).forEach(i -> data1[i] = (byte) (i % 251));

    var inputs = Map.of("input1", InputBlobDefinition.from("blobName1", data1));

    // When
    var inputHandles = blobService.createBlobs(inputs);

    // Then
    assertThat(inputHandles).hasSize(1);
    assertThat(inputHandles).containsOnlyKeys("input1");

    var blobHandle = inputHandles.get("input1");
    assertThat(blobHandle.name()).isEqualTo("blobName1");

    var blobInfo = blobHandle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(blobInfo.id()).isNotNull();
    assertThat(blobInfo.status()).isNotNull();

    assertThat(agentGrpcMock.uploadedBlobs).isNull();
    assertThat(agentGrpcMock.blobMetadata.sessionId()).isEqualTo(sessionId.asString());
    assertThat(agentGrpcMock.blobMetadata.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.blobMetadata.names()).containsExactly(blobHandle.name());
    assertThat(agentGrpcMock.notifyBlobs.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.notifyBlobs.ids()).containsExactly(Map.entry(sessionId.asString(), blobInfo.id().asString()));

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
    assertThat(sessionId.asString()).isEqualTo("sessionId");

    var blobInfo = blobHandle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(blobInfo.id()).isNotNull();
    assertThat(blobInfo.status()).isNotNull();

    assertThat(agentGrpcMock.uploadedBlobs).isNull();
    assertThat(agentGrpcMock.blobMetadata.sessionId()).isEqualTo(sessionId.asString());
    assertThat(agentGrpcMock.blobMetadata.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.blobMetadata.names()).containsExactly(blobHandle.name());
    assertThat(agentGrpcMock.notifyBlobs.communicationToken()).isEqualTo("communicationToken");
    assertThat(agentGrpcMock.notifyBlobs.ids()).containsExactly(Map.entry(sessionId.asString(), blobInfo.id().asString()));

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

    var output2Handle = outputHandles.get("output2");
    assertThat(output2Handle.name()).isEmpty();

    var blobInfo1 = output1Handle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(blobInfo1.id()).isNotNull();
    assertThat(blobInfo1.status()).isNotNull();

    var blobInfo2 = output2Handle.deferredBlobInfo().toCompletableFuture().join();
    assertThat(blobInfo2.id()).isNotNull();
    assertThat(blobInfo2.status()).isNotNull();

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

  @Test
  @DisplayName("should upload several small blobs in one CreateResults request")
  void should_upload_small_blobs_in_one_request() {
    // Given
    var inputs = Map.of(
      "a", InputBlobDefinition.from("blobA", new byte[1024]),
      "b", InputBlobDefinition.from("blobB", new byte[2048]),
      "c", InputBlobDefinition.from("blobC", new byte[10]));

    // When
    var handles = blobService.createBlobs(inputs);

    // Then
    assertThat(agentGrpcMock.createResultsCalls).isEqualTo(1);
    assertThat(agentGrpcMock.uploadedBlobs.blobs()).containsOnlyKeys("blobA", "blobB", "blobC");
    assertThat(handles).containsOnlyKeys("a", "b", "c");
    assertThat(handles.get("a").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blobA-id");
    assertThat(handles.get("b").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blobB-id");
    assertThat(handles.get("c").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blobC-id");
  }

  @Test
  @DisplayName("should pack inline blobs by first-fit decreasing within the request budget")
  void should_pack_inline_blobs_within_request_budget() {
    // Given: 0.6 + 0.3 fits, 0.6 + 0.6 does not
    var large = MAX_REQUEST_SIZE * 6 / 10;
    var small = MAX_REQUEST_SIZE * 3 / 10;
    Map<String, InputBlobDefinition> inputs = Map.of(
      "large1", InputBlobDefinition.from("large1", new byte[large]),
      "large2", InputBlobDefinition.from("large2", new byte[large]),
      "small1", InputBlobDefinition.from("small1", new byte[small]),
      "small2", InputBlobDefinition.from("small2", new byte[small]));

    // When
    var groups = GrpcBlobService.packBySize(inputs, MAX_REQUEST_SIZE);

    // Then
    assertThat(groups).hasSize(2);
    assertThat(groups).allSatisfy(group -> {
      assertThat(group).hasSize(2);
      assertThat(group.stream().mapToLong(entry -> GrpcBlobService.inlineSize(entry.getValue())).sum()).isLessThanOrEqualTo(MAX_REQUEST_SIZE);
    });
  }

  @Test
  @DisplayName("should split inline blobs whose data fits the limit but whose request does not")
  void should_split_blobs_when_envelope_exceeds_limit() {
    // Given: two blobs of exactly half the limit, which fit together by data size only
    Map<String, InputBlobDefinition> inputs = Map.of(
      "half1", InputBlobDefinition.from("half1", new byte[MAX_REQUEST_SIZE / 2]),
      "half2", InputBlobDefinition.from("half2", new byte[MAX_REQUEST_SIZE / 2]));

    // When
    blobService.createBlobs(inputs);

    // Then
    assertThat(agentGrpcMock.createResultsCalls).isEqualTo(2);
    assertThat(agentGrpcMock.requestSizes).allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(MAX_REQUEST_SIZE));
  }

  @Test
  @DisplayName("should keep every request within the Agent limit for many blobs near the boundary")
  void should_keep_requests_within_limit_for_many_blobs() {
    // Given: 1000 blobs whose data adds up to just below the limit, but not with the framing of each entry
    var inputs = new HashMap<String, InputBlobDefinition>();
    IntStream.range(0, 1000).forEach(i -> inputs.put("blob" + i, InputBlobDefinition.from("blob" + i, new byte[4190])));

    // When
    var handles = blobService.createBlobs(inputs);

    // Then
    assertThat(agentGrpcMock.createResultsCalls).isEqualTo(2);
    assertThat(agentGrpcMock.requestSizes).allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(MAX_REQUEST_SIZE));
    assertThat(handles).hasSize(1000);
  }

  @Test
  @DisplayName("should not put more than MAX_BLOBS_PER_REQUEST blobs in one request")
  void should_limit_blob_count_per_request() {
    // Given
    var inputs = new HashMap<String, InputBlobDefinition>();
    IntStream.range(0, MAX_BLOBS_PER_REQUEST + 1).forEach(i -> inputs.put("blob" + i, InputBlobDefinition.from("blob" + i, new byte[1])));

    // When
    var handles = blobService.createBlobs(inputs);

    // Then
    assertThat(agentGrpcMock.createResultsCalls).isEqualTo(2);
    assertThat(handles).hasSize(MAX_BLOBS_PER_REQUEST + 1);
    assertThat(handles.get("blob42").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blob42-id");
  }

  @Test
  @DisplayName("should create metadata with one request per MAX_BLOBS_PER_REQUEST blobs")
  void should_prepare_metadata_in_groups() {
    // Given
    var outputs = new HashMap<String, OutputBlobDefinition>();
    IntStream.range(0, MAX_BLOBS_PER_REQUEST + 1).forEach(i -> outputs.put("out" + i, OutputBlobDefinition.from("out" + i)));

    // When
    var handles = blobService.prepareBlobs(outputs);

    // Then
    assertThat(agentGrpcMock.createResultsMetaDataCalls).isEqualTo(2);
    assertThat(handles).hasSize(MAX_BLOBS_PER_REQUEST + 1);
    assertThat(handles.get("out1000").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("out1000-id");
  }

  @Test
  @DisplayName("should send no request for empty maps")
  void should_send_no_request_for_empty_maps() {
    // When
    blobService.createBlobs(Map.of());
    blobService.prepareBlobs(Map.of());

    // Then
    assertThat(agentGrpcMock.createResultsCalls).isZero();
    assertThat(agentGrpcMock.createResultsMetaDataCalls).isZero();
  }
}
