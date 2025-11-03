
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

import static org.assertj.core.api.Assertions.assertThat;

class OutputBlobDefinitionTest {

  @Test
  void should_have_empty_name_and_automatic_deletion_when_created_with_from_string() {
    // When
    var definition = OutputBlobDefinition.from("output_name");

    // Then
    assertThat(definition.name()).isEqualTo("output_name");
    assertThat(definition.manualDeletion()).isFalse();
  }

  @Test
  void should_have_specified_name_and_deletion_policy_when_created_with_from_string_and_deletion_policy() {
    // When
    var definition = OutputBlobDefinition.from("output_name", true);

    // Then
    assertThat(definition.name()).isEqualTo("output_name");
    assertThat(definition.manualDeletion()).isTrue();
  }

  @Test
  void should_convert_null_name_to_empty_string_when_using_from_string() {
    // When
    var definition = OutputBlobDefinition.from(null);

    // Then
    assertThat(definition.name()).isEmpty();
  }

  @Test
  void should_create_empty_named_output_with_automatic_deletion_by_default() {
    // When
    var definition = OutputBlobDefinition.from("");

    // Then
    assertThat(definition.name()).isEmpty();
    assertThat(definition.manualDeletion()).isFalse();
  }
}
