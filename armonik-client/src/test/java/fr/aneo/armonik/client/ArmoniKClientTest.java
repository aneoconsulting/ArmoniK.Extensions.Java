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

import fr.aneo.armonik.client.blob.BlobDefinition;
import fr.aneo.armonik.client.blob.BlobHandle;
import fr.aneo.armonik.client.blob.BlobHandlesAllocation;
import fr.aneo.armonik.client.payload.PayloadSerializer;
import fr.aneo.armonik.client.session.SessionHandle;
import fr.aneo.armonik.client.task.TaskDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static fr.aneo.armonik.client.blob.BlobHandleFixture.blobHandle;
import static fr.aneo.armonik.client.session.SessionHandleFixture.sessionHandle;
import static fr.aneo.armonik.client.task.TaskConfiguration.defaultConfiguration;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ArmoniKClientTest {

  private PayloadSerializer payloadSerializer;
  private Services services;

  @BeforeEach
  void setUp() {
    payloadSerializer = mock(PayloadSerializer.class);
    services = mock(Services.class, withSettings().defaultAnswer(RETURNS_DEEP_STUBS));
  }

  @Test
  void should_create_session_when_client_is_created() {
    // Given
    var session = sessionHandle();
    var taskConfiguration = defaultConfiguration();
    var partitionIds = Set.of("partition1");
    when(services.sessions().createSession(any(), any())).thenReturn(session);

    // When
    var client = new ArmoniKClient(Set.of("partition1"), taskConfiguration, services);

    // Then
    verify(services.sessions()).createSession(partitionIds, taskConfiguration);
    assertThat(client.sessionHandle).isEqualTo(session);
  }

  @Test
  void should_execute_submission_pipeline_successfully() {
    // Given
    var session = sessionHandle();
    var taskDefinition = new TaskDefinition()
      .withInput("name", BlobDefinition.from("John".getBytes(UTF_8)))
      .withInput("age", blobHandle(session))
      .withOutput("result1")
      .withOutput("result2");

    var allocation = new BlobHandlesAllocation(
      blobHandle(session),
      Map.of("name", blobHandle(session)),
      Map.of("result1", blobHandle(session), "result2", blobHandle(session))
    );

    var payloadDefinition = BlobDefinition.from("{}".getBytes(UTF_8));

    when(services.blobs().uploadBlobData(any(), any())).thenReturn(completedFuture(null));
    when(services.blobs().allocateBlobHandles(eq(session), any(TaskDefinition.class))).thenReturn(allocation);
    when(payloadSerializer.serialize(anyMap(), anyMap())).thenReturn(payloadDefinition);
    when(services.sessions().createSession(any(), any())).thenReturn(session);

    var client = new ArmoniKClient(Set.of("partition1"), defaultConfiguration(), payloadSerializer, services);

    // When
    client.submitTask(taskDefinition);

    // Then
    assertPayloadSerialization(allocation, taskDefinition);
    assertUploadsForInlineInputsAndPayload(allocation, taskDefinition, payloadDefinition);
    assertTaskSubmissionWiring(session, allocation, taskDefinition);
  }

  @SuppressWarnings("unchecked")
  private void assertTaskSubmissionWiring(SessionHandle sessionHandle, BlobHandlesAllocation allocation, TaskDefinition taskDefinition) {
    ArgumentCaptor<Map<String, BlobHandle>> inputsCap = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Map<String, BlobHandle>> outputsCap = ArgumentCaptor.forClass(Map.class);
    var payloadCap = ArgumentCaptor.forClass(BlobHandle.class);

    verify(services.tasks()).submitTask(eq(sessionHandle), inputsCap.capture(), outputsCap.capture(), payloadCap.capture(), any());

    assertThat(inputsCap.getValue())
      .containsEntry("name", allocation.inputHandlesByName().get("name"))
      .containsEntry("age", taskDefinition.inputHandles().get("age"))
      .hasSize(2);

    assertThat(outputsCap.getValue())
      .containsEntry("result2", allocation.outputHandlesByName().get("result2"))
      .containsEntry("result2", allocation.outputHandlesByName().get("result2"))
      .hasSize(2);

    assertThat(payloadCap.getValue()).isSameAs(allocation.payloadHandle());
  }

  @SuppressWarnings("unchecked")
  private void assertPayloadSerialization(BlobHandlesAllocation allocation, TaskDefinition taskDefinition) {
    ArgumentCaptor<Map<String, UUID>> payloadInputsCap = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Map<String, UUID>> payloadOutputsCap = ArgumentCaptor.forClass(Map.class);

    verify(payloadSerializer).serialize(payloadInputsCap.capture(), payloadOutputsCap.capture());

    assertThat(payloadInputsCap.getValue())
      .containsEntry("name", allocation.inputHandlesByName().get("name").metadata().toCompletableFuture().join().id())
      .containsEntry("age", taskDefinition.inputHandles().get("age").metadata().toCompletableFuture().join().id())
      .hasSize(2);

    assertThat(payloadOutputsCap.getValue())
      .containsEntry("result1", allocation.outputHandlesByName().get("result1").metadata().toCompletableFuture().join().id())
      .containsEntry("result2", allocation.outputHandlesByName().get("result2").metadata().toCompletableFuture().join().id())
      .hasSize(2);
  }

  private void assertUploadsForInlineInputsAndPayload(BlobHandlesAllocation allocation, TaskDefinition taskDefinition, BlobDefinition payloadDefinition) {
    verify(services.blobs()).uploadBlobData(
      same(allocation.inputHandlesByName().get("name")),
      same(taskDefinition.inputDefinitions().get("name"))
    );

    verify(services.blobs()).uploadBlobData(
      same(allocation.payloadHandle()),
      same(payloadDefinition)
    );

    verify(services.blobs(), never())
      .uploadBlobData(same(taskDefinition.inputHandles().get("age")), any());
  }
}
