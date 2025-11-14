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
package fr.aneo.armonik.worker.domain.internal;

import fr.aneo.armonik.worker.domain.BlobId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadTest {

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
