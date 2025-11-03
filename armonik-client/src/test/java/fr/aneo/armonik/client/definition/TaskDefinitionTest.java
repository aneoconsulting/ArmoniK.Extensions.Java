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
package fr.aneo.armonik.client.definition;

import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static fr.aneo.armonik.client.model.TestDataFactory.blobHandle;
import static org.assertj.core.api.Assertions.assertThat;

class TaskDefinitionTest {

  @Test
  void should_replace_input_handle_by_new_one_when_using_an_existing_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", blobHandle("sessionId", "blobId1"));
    var handle2 = blobHandle("sessionId", "blobId2");

    // When
    taskDefinition.withInput("prop1", handle2);

    // Then
    assertThat(taskDefinition.inputHandles()).hasSize(1);
    assertThat(taskDefinition.inputHandles().get("prop1").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blobId2");
  }

  @Test
  void should_replace_inline_input_by_new_one_when_using_an_existing_name() throws IOException {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", InputBlobDefinition.from("Hello".getBytes()));

    // When
    taskDefinition.withInput("prop1", InputBlobDefinition.from("World".getBytes()));

    // Then
    assertThat(taskDefinition.inputDefinitions()).hasSize(1);
    assertThat(taskDefinition.inputDefinitions().get("prop1").data().stream().readAllBytes()).asString().isEqualTo("World");
  }

  @Test
  void should_remove_input_handle_when_adding_inline_input_with_the_same_name() throws IOException {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", blobHandle("sessionId", "blobId1"));

    // When
    taskDefinition.withInput("prop1", InputBlobDefinition.from("World".getBytes()));

    // Then
    assertThat(taskDefinition.inputHandles()).isEmpty();
    assertThat(taskDefinition.inputDefinitions()).hasSize(1);
    assertThat(taskDefinition.inputDefinitions().get("prop1").data().stream().readAllBytes()).asString().isEqualTo("World");
  }

  @Test
  void should_remove_inline_input_when_adding_handle_input_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", InputBlobDefinition.from("Hello".getBytes()));

    // When
    taskDefinition.withInput("prop1", blobHandle("sessionId", "blobId"));

    // Then
    assertThat(taskDefinition.inputHandles()).hasSize(1);
    assertThat(taskDefinition.inputHandles().get("prop1").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blobId");
  }

  @Test
  void should_create_output_with_default_name_and_manual_deletion_when_using_simple_withOutput() {
    // Given
    var taskDefinition = new TaskDefinition();

    // When
    taskDefinition.withOutput("result");

    // Then
    assertThat(taskDefinition.outputDefinitions()).hasSize(1);
    assertThat(taskDefinition.outputDefinitions().get("result").name()).isEmpty();
    assertThat(taskDefinition.outputDefinitions().get("result").manualDeletion()).isFalse();
  }
}
