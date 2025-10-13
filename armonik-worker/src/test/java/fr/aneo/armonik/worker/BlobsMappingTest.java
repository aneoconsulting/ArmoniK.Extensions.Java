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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobsMappingTest {

  @Test
  @DisplayName("Should parse valid JSON with inputs and outputs")
  void should_parse_valid_json_with_inputs_and_outputs() {
    // Given
    var json = """
      {
        "inputs": {
          "input1": "input1-id",
          "input2": "input2-id"
        },
        "outputs": {
          "output1": "output1-id",
          "output2": "output2-id"
        }
      }
      """;

    // When
    var mapping = BlobsMapping.fromJson(json);

    // Then
    assertThat(mapping.inputsMapping()).containsOnlyKeys("input1", "input2");
    assertThat(mapping.inputsMapping().get("input1")).isEqualTo("input1-id");
    assertThat(mapping.inputsMapping().get("input2")).isEqualTo("input2-id");

    assertThat(mapping.outputsMapping()).containsOnlyKeys("output1", "output2");
    assertThat(mapping.outputsMapping().get("output1")).isEqualTo("output1-id");
    assertThat(mapping.outputsMapping().get("output2")).isEqualTo("output2-id");
  }

  @Test
  @DisplayName("Should parse valid JSON with empty inputs and outputs")
  void should_parse_valid_json_with_empty_inputs_and_outputs() {
    // Given
    var json = """
      {
        "inputs": {},
        "outputs": {}
      }
      """;

    // When
    var mapping = BlobsMapping.fromJson(json);

    // Then
    assertThat(mapping.inputsMapping()).isEmpty();
    assertThat(mapping.outputsMapping()).isEmpty();
  }

  @Test
  @DisplayName("Should throw IllegalArgumentException when JSON is null")
  void should_throw_exception_when_json_is_null() {
    // When - Then
    assertThatThrownBy(() -> BlobsMapping.fromJson(null))
      .isInstanceOf(NullPointerException.class)
      .hasMessageContaining("jsonString cannot be null");
  }

  @Test
  @DisplayName("Should throw IllegalArgumentException when JSON does not contain 'inputs' key")
  void should_throw_exception_when_json_missing_inputs_key() {
    // Given
    var json = """
      {
        "outputs": {
          "output1": "output1-id"
        }
      }
      """;

    // When - Then
    assertThatThrownBy(() -> BlobsMapping.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("must contain 'inputs' key");
  }

  @Test
  @DisplayName("Should throw IllegalArgumentException when JSON does not contain 'outputs' key")
  void should_throw_exception_when_json_missing_outputs_key() {
    // Given
    var json = """
      {
        "inputs": {
          "input1": "input1-id"
        }
      }
      """;

    // When - Then
    assertThatThrownBy(() -> BlobsMapping.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("must contain 'outputs' key");
  }

  @Test
  @DisplayName("Should throw IllegalArgumentException when 'inputs' value is null")
  void should_throw_exception_when_inputs_value_is_null() {
    // Given
    var json = """
      {
        "inputs": null,
        "outputs": {}
      }
      """;

    // When - Then
    assertThatThrownBy(() -> BlobsMapping.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("'inputs' value cannot be null");
  }

  @Test
  @DisplayName("Should throw IllegalArgumentException when 'outputs' value is null")
  void should_throw_exception_when_outputs_value_is_null() {
    // Given
    var json = """
      {
        "inputs": {},
        "outputs": null
      }
      """;

    // When - Then
    assertThatThrownBy(() -> BlobsMapping.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("'outputs' value cannot be null");
  }

  @Test
  @DisplayName("Should throw IllegalArgumentException when JSON is empty")
  void should_throw_exception_when_json_is_empty() {
    // Given
    var json = "{}";

    // When - Then
    assertThatThrownBy(() -> BlobsMapping.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("must contain 'inputs' key");
  }

  @Test
  @DisplayName("Should throw IllegalArgumentException when JSON is malformed")
  void should_throw_exception_when_json_is_malformed() {
    // Given
    var json = "{ invalid json }";

    // When - Then
    assertThatThrownBy(() -> BlobsMapping.fromJson(json))
      .isInstanceOf(com.google.gson.JsonSyntaxException.class);
  }
}
