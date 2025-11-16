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
package fr.aneo.armonik.worker.domain.definition;

import fr.aneo.armonik.worker.domain.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.blob.OutputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.task.TaskDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static fr.aneo.armonik.worker.domain.TestDataFactory.*;
import static org.assertj.core.api.Assertions.assertThat;

class TaskDefinitionTest {

  @Test
  @DisplayName("should replace input handle by new one when using the same name")
  void should_replace_input_handle_by_new_one_when_using_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", blobHandle("blobId1"));
    var handle2 = blobHandle("blobId2");

    // When
    taskDefinition.withInput("prop1", handle2);

    // Then
    assertThat(taskDefinition.inputHandles()).hasSize(1);
    assertThat(taskDefinition.inputHandles().get("prop1").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blobId2");
  }

  @Test
  @DisplayName("should remove input handle when adding inline input with the same name")
  void should_remove_input_handle_when_adding_inline_input_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", blobHandle("blobId1"));

    // When
    taskDefinition.withInput("prop1", InputBlobDefinition.from("World".getBytes()));

    // Then
    assertThat(taskDefinition.inputHandles()).isEmpty();
    assertThat(taskDefinition.taskInputs()).isEmpty();
    assertThat(taskDefinition.inputDefinitions()).hasSize(1);
    assertThat(taskDefinition.inputDefinitions().get("prop1").asStream()).hasBinaryContent("World".getBytes());
  }

  @Test
  @DisplayName("should remove input handle when adding task input with the same name")
  void should_remove_input_handle_input_when_adding_task_input_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", blobHandle("blobId"));
    var taskInput = taskInput("input1-id", "input1");

    // When
    taskDefinition.withInput("prop1", taskInput);

    // Then
    assertThat(taskDefinition.inputHandles()).isEmpty();
    assertThat(taskDefinition.inputDefinitions()).isEmpty();
    assertThat(taskDefinition.taskInputs()).hasSize(1);
    assertThat(taskDefinition.taskInputs().get("prop1").id()).isEqualTo(taskInput.id());
  }


  @Test
  @DisplayName("should replace inline input by new one when using the same name")
  void should_replace_inline_input_by_new_one_when_using_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", InputBlobDefinition.from("Hello".getBytes()));

    // When
    taskDefinition.withInput("prop1", InputBlobDefinition.from("World".getBytes()));

    // Then
    assertThat(taskDefinition.inputDefinitions()).hasSize(1);
    assertThat(taskDefinition.inputDefinitions().get("prop1").asStream()).hasBinaryContent("World".getBytes());
  }

  @Test
  @DisplayName("should remove inline input when adding input handle with the same name")
  void should_remove_inline_input_when_adding_input_handle_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", InputBlobDefinition.from("Hello".getBytes()));

    // When
    taskDefinition.withInput("prop1", blobHandle("blobId"));

    // Then
    assertThat(taskDefinition.inputDefinitions()).isEmpty();
    assertThat(taskDefinition.taskInputs()).isEmpty();
    assertThat(taskDefinition.inputHandles()).hasSize(1);
    assertThat(taskDefinition.inputHandles().get("prop1").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blobId");
  }

  @Test
  @DisplayName("should remove inline input when adding task input with the same name")
  void should_remove_inline_input_when_adding_task_input_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", InputBlobDefinition.from("Hello".getBytes()));

    // When
    taskDefinition.withInput("prop1", taskInput("input1-id", "input1"));

    // Then
    assertThat(taskDefinition.inputDefinitions()).isEmpty();
    assertThat(taskDefinition.inputHandles()).isEmpty();
    assertThat(taskDefinition.taskInputs()).hasSize(1);
  }

  @Test
  @DisplayName("should replace task input by new one when using the same name")
  void should_replace_task_input_by_new_one_when_using_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", taskInput("input1-id", "input1"));
    var taskInput2 = taskInput("input2-id", "input2");

    // When
    taskDefinition.withInput("prop1", taskInput2);

    // Then
    assertThat(taskDefinition.taskInputs()).hasSize(1);
    assertThat(taskDefinition.taskInputs().get("prop1").id()).isEqualTo(taskInput2.id());
  }

  @Test
  @DisplayName("should remove task input when adding inline input with the same name")
  void should_remove_task_input_when_adding_inline_input_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", taskInput("input1-id", "input1"));

    // When
    taskDefinition.withInput("prop1", InputBlobDefinition.from("World".getBytes()));

    // Then
    assertThat(taskDefinition.taskInputs()).isEmpty();
    assertThat(taskDefinition.inputHandles()).isEmpty();
    assertThat(taskDefinition.inputDefinitions()).hasSize(1);
    assertThat(taskDefinition.inputDefinitions().get("prop1").asStream()).hasBinaryContent("World".getBytes());
  }

  @Test
  @DisplayName("should remove task input when adding handle input with the same name")
  void should_remove_task_input_when_adding_handle_input_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withInput("prop1", taskInput("input1-id", "input1"));

    // When
    taskDefinition.withInput("prop1", blobHandle("blobId"));

    // Then
    assertThat(taskDefinition.taskInputs()).isEmpty();
    assertThat(taskDefinition.inputDefinitions()).isEmpty();
    assertThat(taskDefinition.inputHandles()).hasSize(1);
    assertThat(taskDefinition.inputHandles().get("prop1").deferredBlobInfo().toCompletableFuture().join().id().asString()).isEqualTo("blobId");
  }

  @Test
  @DisplayName("should replace task output by new one when using the same name")
  void should_replace_task_output_by_new_one_when_using_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withOutput("result", taskOutput("output1-id", "output1"));
    var taskOutput2 = taskOutput("output2-id", "output2");

    // When
    taskDefinition.withOutput("result", taskOutput2);

    // Then
    assertThat(taskDefinition.taskOutputs()).hasSize(1);
    assertThat(taskDefinition.taskOutputs().get("result")).isEqualTo(taskOutput2);
  }

  @Test
  @DisplayName("should remove task output when adding output definition with the same name")
  void should_remove_task_output_when_adding_output_definition_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withOutput("result", taskOutput("output1-id", "output1"));

    // When
    taskDefinition.withOutput("result", OutputBlobDefinition.from("newOutput"));

    // Then
    assertThat(taskDefinition.taskOutputs()).isEmpty();
    assertThat(taskDefinition.outputDefinitions()).hasSize(1);
    assertThat(taskDefinition.outputDefinitions().get("result").name()).isEqualTo("newOutput");
  }

  @Test
  @DisplayName("should remove output definition when adding task output with the same name")
  void should_remove_output_definition_when_adding_task_output_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withOutput("result", OutputBlobDefinition.from("myOutput"));

    // When
    taskDefinition.withOutput("result", taskOutput("output1-id", "output1"));

    // Then
    assertThat(taskDefinition.outputDefinitions()).isEmpty();
    assertThat(taskDefinition.taskOutputs()).hasSize(1);
  }

  @Test
  @DisplayName("should remove task output when adding default output with the same name")
  void should_remove_task_output_when_adding_default_output_with_the_same_name() {
    // Given
    var taskDefinition = new TaskDefinition().withOutput("result", taskOutput("output1-id", "output1"));

    // When
    taskDefinition.withOutput("result");

    // Then
    assertThat(taskDefinition.taskOutputs()).isEmpty();
    assertThat(taskDefinition.outputDefinitions()).hasSize(1);
    assertThat(taskDefinition.outputDefinitions().get("result").name()).isEmpty();
  }
}
