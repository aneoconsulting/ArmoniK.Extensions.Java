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
package fr.aneo.armonik.worker.domain;

import fr.aneo.armonik.worker.domain.internal.BlobWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TaskOutputTest {
  private final BlobId blobId = BlobId.from("id");

  @Test
  @DisplayName("writes byte array to blob writer")
  void writes_byte_array_to_blob_writer() {
    // Given
    BlobWriter writer = mock(BlobWriter.class);
    var output = new TaskOutput(blobId, "result", writer);
    var data = randomBytes(32);

    // When
    output.write(data);

    // Then
    var idCaptor = ArgumentCaptor.forClass(BlobId.class);
    var dataCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(writer).write(idCaptor.capture(), dataCaptor.capture());

    assertThat(idCaptor.getValue()).isEqualTo(blobId);
    assertThat(dataCaptor.getValue()).containsExactly(data);
  }

  @Test
  @DisplayName("writes UTF-8 text as bytes to blob writer")
  void writes_utf8_text_as_bytes_to_blob_writer() {
    // Given
    BlobWriter writer = mock(BlobWriter.class);
    var output = new TaskOutput(blobId, "greeting", writer);
    var text = "héllo 🌍";

    // When
    output.write(text, UTF_8);

    // Then
    var idCaptor = ArgumentCaptor.forClass(BlobId.class);
    var dataCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(writer).write(idCaptor.capture(), dataCaptor.capture());

    assertThat(idCaptor.getValue()).isEqualTo(blobId);
    assertThat(new String(dataCaptor.getValue(), UTF_8)).isEqualTo(text);
  }

  @Test
  @DisplayName("writes input stream to blob writer")
  void writes_input_stream_to_blob_writer() {
    // Given
    BlobWriter writer = mock(BlobWriter.class);
    var output = new TaskOutput(blobId, "data", writer);
    var data = randomBytes(1024);
    var inputStream = new ByteArrayInputStream(data);

    // When
    output.write(inputStream);

    // Then
    verify(writer).write(blobId, inputStream);
  }

  private static byte[] randomBytes(int size) {
    byte[] data = new byte[size];
    new Random(1234).nextBytes(data);
    return data;
  }
}
