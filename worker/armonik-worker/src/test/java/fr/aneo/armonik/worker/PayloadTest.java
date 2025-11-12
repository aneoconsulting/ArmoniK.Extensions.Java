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

import java.io.IOException;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadTest {

  @Test
  @DisplayName("should parse valid JSON with inputs and outputs")
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
    var payload = Payload.fromJson(json);

    // Then
    assertThat(payload.inputsMapping()).containsOnlyKeys("input1", "input2");
    assertThat(payload.inputsMapping().get("input1").asString()).isEqualTo("input1-id");
    assertThat(payload.inputsMapping().get("input2").asString()).isEqualTo("input2-id");

    assertThat(payload.outputsMapping()).containsOnlyKeys("output1", "output2");
    assertThat(payload.outputsMapping().get("output1").asString()).isEqualTo("output1-id");
    assertThat(payload.outputsMapping().get("output2").asString()).isEqualTo("output2-id");
  }

  @Test
  @DisplayName("should parse valid JSON with empty inputs and outputs")
  void should_parse_valid_json_with_empty_inputs_and_outputs() {
    // Given
    var json = """
      {
        "inputs": {},
        "outputs": {}
      }
      """;

    // When
    var payload = Payload.fromJson(json);

    // Then
    assertThat(payload.inputsMapping()).isEmpty();
    assertThat(payload.outputsMapping()).isEmpty();
  }

  @Test
  @DisplayName("should throw NullPointerException when JSON is null")
  void should_throw_exception_when_json_is_null() {
    // When - Then
    assertThatThrownBy(() -> Payload.fromJson(null))
      .isInstanceOf(NullPointerException.class)
      .hasMessageContaining("jsonString cannot be null");
  }

  @Test
  @DisplayName("should throw IllegalArgumentException when JSON does not contain inputs key")
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
    assertThatThrownBy(() -> Payload.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("must contain 'inputs' key");
  }

  @Test
  @DisplayName("should throw IllegalArgumentException when JSON does not contain outputs key")
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
    assertThatThrownBy(() -> Payload.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("must contain 'outputs' key");
  }

  @Test
  @DisplayName("should throw IllegalArgumentException when inputs value is null")
  void should_throw_exception_when_inputs_value_is_null() {
    // Given
    var json = """
      {
        "inputs": null,
        "outputs": {}
      }
      """;

    // When - Then
    assertThatThrownBy(() -> Payload.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("'inputs' value cannot be null");
  }

  @Test
  @DisplayName("should throw IllegalArgumentException when outputs value is null")
  void should_throw_exception_when_outputs_value_is_null() {
    // Given
    var json = """
      {
        "inputs": {},
        "outputs": null
      }
      """;

    // When - Then
    assertThatThrownBy(() -> Payload.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("'outputs' value cannot be null");
  }

  @Test
  @DisplayName("should throw IllegalArgumentException when JSON is empty")
  void should_throw_exception_when_json_is_empty() {
    // Given
    var json = "{}";

    // When - Then
    assertThatThrownBy(() -> Payload.fromJson(json))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("must contain 'inputs' key");
  }

  @Test
  @DisplayName("should throw JsonSyntaxException when JSON is malformed")
  void should_throw_exception_when_json_is_malformed() {
    // Given
    var json = "{ invalid json }";

    // When - Then
    assertThatThrownBy(() -> Payload.fromJson(json))
      .isInstanceOf(com.google.gson.JsonSyntaxException.class);
  }

  @Test
  @DisplayName("should create payload from blob ID maps")
  void should_create_payload_from_blob_id_maps() {
    // Given
    var inputs = Map.of(
      "input1", BlobId.from("input1-id"),
      "input2", BlobId.from("input2-id")
    );
    var outputs = Map.of(
      "output1", BlobId.from("output1-id"),
      "output2", BlobId.from("output2-id")
    );

    // When
    var payload = Payload.from(inputs, outputs);

    // Then
    assertThat(payload.inputsMapping()).containsOnlyKeys("input1", "input2");
    assertThat(payload.inputsMapping().get("input1").asString()).isEqualTo("input1-id");
    assertThat(payload.inputsMapping().get("input2").asString()).isEqualTo("input2-id");

    assertThat(payload.outputsMapping()).containsOnlyKeys("output1", "output2");
    assertThat(payload.outputsMapping().get("output1").asString()).isEqualTo("output1-id");
    assertThat(payload.outputsMapping().get("output2").asString()).isEqualTo("output2-id");
  }

  @Test
  @DisplayName("should convert payload to input blob definition with correct JSON content")
  void should_convert_payload_to_input_blob_definition() throws IOException {
    // Given
    var inputs = Map.of("input1", BlobId.from("input1-id"));
    var outputs = Map.of("output1", BlobId.from("output1-id"));
    var payload = Payload.from(inputs, outputs);

    // When
    var blobDefinition = payload.asInputBlobDefinition();

    // Then
    assertThat(blobDefinition).isNotNull();
    assertThat(blobDefinition.name()).isEqualTo("payload");

    try (var stream = blobDefinition.asStream()) {
      var jsonContent = new String(stream.readAllBytes(), UTF_8);
      var reparsedPayload = Payload.fromJson(jsonContent);

      assertThat(reparsedPayload.inputsMapping()).containsExactlyInAnyOrderEntriesOf(payload.inputsMapping());
      assertThat(reparsedPayload.outputsMapping()).containsExactlyInAnyOrderEntriesOf(payload.outputsMapping());
    }
  }

  @Test
  @DisplayName("should throw IllegalArgumentException when inputs are empty")
  void should_throw_exception_when_inputs_are_empty() {
    // Given
    var inputs = Map.<String, BlobId>of();
    var outputs = Map.of("input1", BlobId.from("input1-id"));

    // When - Then
    assertThatThrownBy(() -> Payload.from(inputs, outputs))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw IllegalArgumentException when outputs are empty")
  void should_throw_exception_when_outputs_are_empty() {
    // Given
    var inputs = Map.of("input1", BlobId.from("input1-id"));
    var outputs = Map.<String, BlobId>of();

    // When - Then
    assertThatThrownBy(() -> Payload.from(inputs, outputs))
      .isInstanceOf(IllegalArgumentException.class);
  }
}
