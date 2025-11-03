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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

class InMemoryBlobDataTest {

  @Test
  @DisplayName("should create in memory blob data from byte array")
  void should_create_in_memory_blob_data_from_byte_array() {
    // Given
    var data = "test data".getBytes();

    // When
    var blobData = InMemoryBlobData.from(data);

    // Then
    assertThat(blobData).isNotNull();
    assertThat(blobData.data()).isEqualTo(data);
  }

  @Test
  @DisplayName("should throw null pointer exception when data is null")
  void should_throw_null_pointer_exception_when_data_is_null() {
    // When / Then
    assertThatThrownBy(() -> InMemoryBlobData.from(null))
      .isInstanceOf(NullPointerException.class)
      .hasMessageContaining("data must not be null");
  }

  @Test
  @DisplayName("should return readable stream")
  void should_return_readable_stream() throws IOException {
    // Given
    var data = "stream test".getBytes();
    var blobData = InMemoryBlobData.from(data);

    // When
    try (var stream = blobData.stream()) {
      var readData = stream.readAllBytes();

      // Then
      assertThat(readData).isEqualTo(data);
    }
  }

  @Test
  @DisplayName("should return independent streams on multiple calls")
  void should_return_independent_streams_on_multiple_calls() throws IOException {
    // Given
    var data = "independent streams".getBytes();
    var blobData = InMemoryBlobData.from(data);

    // When - read from first stream
    byte[] firstRead;
    try (var stream = blobData.stream()) {
      firstRead = stream.readAllBytes();
    }

    // When - read from second stream
    byte[] secondRead;
    try (var stream = blobData.stream()) {
      secondRead = stream.readAllBytes();
    }

    // Then
    assertThat(firstRead).isEqualTo(data);
    assertThat(secondRead).isEqualTo(data);
  }
}
