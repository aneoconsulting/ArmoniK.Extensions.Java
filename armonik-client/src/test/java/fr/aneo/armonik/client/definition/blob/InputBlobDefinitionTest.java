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
package fr.aneo.armonik.client.definition.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class InputBlobDefinitionTest {

  @Test
  void should_have_empty_name_and_automatic_deletion_when_created_from_byte_array() {
    // When
    var definition = InputBlobDefinition.from("test data".getBytes());

    // Then
    assertThat(definition.name()).isEmpty();
    assertThat(definition.manualDeletion()).isFalse();
  }

  @Test
  void should_have_empty_name_and_automatic_deletion_when_created_from_blob_data() {
    // Given
    var blobData = InMemoryBlobData.from("test data".getBytes());

    // When
    var definition = InputBlobDefinition.from(blobData);

    // Then
    assertThat(definition.name()).isEmpty();
    assertThat(definition.manualDeletion()).isFalse();
  }

  @Test
  void should_have_empty_name_and_automatic_deletion_when_created_from_file(@TempDir Path tempDir) throws IOException {
    // Given
    var file = tempDir.resolve("test.txt").toFile();
    Files.writeString(file.toPath(), "test data");

    // When
    var definition = InputBlobDefinition.from(file);

    // Then
    assertThat(definition.name()).isEmpty();
    assertThat(definition.manualDeletion()).isFalse();
  }

  @Test
  void should_set_name_when_using_withName() {
    // Given
    var definition = InputBlobDefinition.from("test data".getBytes());

    // When
    definition.withName("custom_name");

    // Then
    assertThat(definition.name()).isEqualTo("custom_name");
  }

  @Test
  void should_convert_null_to_empty_string_when_using_withName() {
    // Given
    var definition = InputBlobDefinition.from("test data".getBytes());

    // When
    definition.withName(null);

    // Then
    assertThat(definition.name()).isEmpty();
  }

  @Test
  void should_enable_manual_deletion_when_using_withManualDeletion_boolean() {
    // Given
    var definition = InputBlobDefinition.from("test data".getBytes());

    // When
    definition.withManualDeletion(true);

    // Then
    assertThat(definition.manualDeletion()).isTrue();
  }

  @Test
  void should_enable_manual_deletion_when_using_withManualDeletion() {
    // Given
    var definition = InputBlobDefinition.from("test data".getBytes());

    // When
    definition.withManualDeletion();

    // Then
    assertThat(definition.manualDeletion()).isTrue();
  }
}
