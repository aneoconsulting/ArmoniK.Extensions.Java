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
package fr.aneo.armonik.client.task;

import fr.aneo.armonik.client.blob.BlobDefinition;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static fr.aneo.armonik.client.blob.BlobHandleFixture.blobHandle;
import static org.assertj.core.api.Assertions.assertThat;

class TaskDefinitionTest {

  @Test
  void should_replace_input_handle_by_new_one_when_using_an_existing_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", blobHandle());
    var secondBlobId = UUID.randomUUID();

    // When
    taskDefinition.withInput("prop1", blobHandle(secondBlobId));

    // Then
    assertThat(taskDefinition.inputHandles()).hasSize(1);
    assertThat(taskDefinition.inputHandles().get("prop1").metadata().toCompletableFuture().join().id()).isEqualTo(secondBlobId);
  }

  @Test
  void should_replace_inline_input_by_new_one_when_using_an_existing_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", BlobDefinition.from("Hello".getBytes()));

    // When
    taskDefinition.withInput("prop1", BlobDefinition.from("World".getBytes()));

    // Then
    assertThat(taskDefinition.inputDefinitions()).hasSize(1);
    assertThat(taskDefinition.inputDefinitions().get("prop1").data()).asString().isEqualTo("World");
  }

  @Test
  void should_remove_input_handle_when_adding_inline_input_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", blobHandle());

    // When
    taskDefinition.withInput("prop1", BlobDefinition.from("World".getBytes()));

    // Then
    assertThat(taskDefinition.inputHandles()).isEmpty();
    assertThat(taskDefinition.inputDefinitions()).hasSize(1);
    assertThat(taskDefinition.inputDefinitions().get("prop1").data()).asString().isEqualTo("World");
  }

  @Test
  void should_remove_inline_input_when_adding_handle_input_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", BlobDefinition.from("Hello".getBytes()));
    var blobId = UUID.randomUUID();

    // When
    taskDefinition.withInput("prop1", blobHandle(blobId));

    // Then
    assertThat(taskDefinition.inputHandles()).hasSize(1);
    assertThat(taskDefinition.inputHandles().get("prop1").metadata().toCompletableFuture().join().id()).isEqualTo(blobId);
  }
}
