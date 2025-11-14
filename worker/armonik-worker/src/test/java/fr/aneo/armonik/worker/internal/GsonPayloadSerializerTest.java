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

import fr.aneo.armonik.worker.domain.BlobId;
import fr.aneo.armonik.worker.domain.internal.Payload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GsonPayloadSerializerTest {
  GsonPayloadSerializer serializer;

  @BeforeEach
  void setUp() {
    serializer = new GsonPayloadSerializer();
  }

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
    var payload = serializer.deserialize(json.getBytes(UTF_8));

    // Then
    assertThat(payload.inputsMapping()).containsExactlyInAnyOrderEntriesOf(
      Map.of(
        "input1", BlobId.from("input1-id"),
        "input2", BlobId.from("input2-id"))
    );

    assertThat(payload.outputsMapping()).containsExactlyInAnyOrderEntriesOf(
      Map.of(
        "output1", BlobId.from("output1-id"),
        "output2", BlobId.from("output2-id"))
    );
  }

  @Test
  @DisplayName("should throw NullPointerException when JSON is null")
  void should_throw_exception_when_json_is_null() {
    // When - Then
    assertThatThrownBy(() -> serializer.deserialize(null))
      .isInstanceOf(NullPointerException.class);
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
    assertThatThrownBy(() -> serializer.deserialize(json.getBytes(UTF_8)))
      .isInstanceOf(IllegalArgumentException.class);
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
    assertThatThrownBy(() -> serializer.deserialize(json.getBytes(UTF_8)))
      .isInstanceOf(IllegalArgumentException.class);
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
    assertThatThrownBy(() -> serializer.deserialize(json.getBytes(UTF_8)))
      .isInstanceOf(IllegalArgumentException.class);
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
    assertThatThrownBy(() -> serializer.deserialize(json.getBytes(UTF_8)))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw IllegalArgumentException when JSON is empty")
  void should_throw_exception_when_json_is_empty() {
    // Given
    var json = "{}";

    // When - Then
    assertThatThrownBy(() -> serializer.deserialize(json.getBytes(UTF_8)))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw JsonSyntaxException when JSON is malformed")
  void should_throw_exception_when_json_is_malformed() {
    // Given
    var json = "{ invalid json }";

    // When - Then
    assertThatThrownBy(() -> serializer.deserialize(json.getBytes(UTF_8)))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("serializes payload to json")
  void serializes_payload_to_json() {
    // Given
    var payload = Payload.from(
      Map.of("input1", BlobId.from("blob-123")),
      Map.of("output1", BlobId.from("blob-456"))
    );

    // When
    byte[] bytes = serializer.serialize(payload);

    // Then
    String json = new String(bytes, StandardCharsets.UTF_8);
    assertThat(json).contains("\"inputs\"");
    assertThat(json).contains("\"input1\":\"blob-123\"");
    assertThat(json).contains("\"outputs\"");
    assertThat(json).contains("\"output1\":\"blob-456\"");
  }

  @Test
  @DisplayName("round trip serialization preserves data")
  void round_trip_serialization_preserves_data() {
    // Given
    var original = Payload.from(
      Map.of("in1", BlobId.from("blob-1"), "in2", BlobId.from("blob-2")),
      Map.of("out1", BlobId.from("blob-3"))
    );

    // When
    byte[] bytes = serializer.serialize(original);
    Payload deserialized = serializer.deserialize(bytes);

    // Then
    assertThat(deserialized).isEqualTo(original);
  }

  @Test
  @DisplayName("throws exception for non-string blob id")
  void throws_exception_for_non_string_blob_id() {
    // Given
    String json = """
      {
        "inputs": {"data": 123},
        "outputs": {"result": "blob-456"}
      }
      """;
    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

    // When/Then
    assertThatThrownBy(() -> serializer.deserialize(bytes))
      .isInstanceOf(IllegalArgumentException.class);
  }

}
