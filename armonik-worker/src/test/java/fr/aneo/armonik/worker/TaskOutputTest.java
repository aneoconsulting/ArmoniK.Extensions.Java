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
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TaskOutputTest {
  @TempDir
  private Path tempDir;
  private final BlobId blobId = BlobId.from("id");

  @Test
  @DisplayName("Should write byte array")
  void should_write_byte_array() throws IOException {
    // Given
    var notifiedId = new AtomicReference<>();
    var path = randomPath();
    var output = new TaskOutput(blobId, "result", path, notifiedId::set);
    var data = randomBytes(32);

    // When
    output.write(data);

    // Then
    var written = Files.readAllBytes(path);
    assertThat(written).containsExactly(data);
    assertThat(notifiedId.get()).isEqualTo(blobId);
  }

  @Test
  @DisplayName("Should write UTF-8 text")
  void should_write_utf8_text() throws IOException {
    // Given
    var notifiedId = new AtomicReference<>();
    var path = randomPath();
    var output = new TaskOutput(blobId, "greeting", path, notifiedId::set);
    var text = "héllo 🌍";

    // When
    output.write(text, UTF_8);

    // Then
    var content = Files.readString(path, UTF_8);
    assertThat(content).isEqualTo(text);
    assertThat(notifiedId.get()).isEqualTo(blobId);
  }

  @Test
  @DisplayName("Should write InputStream")
  void should_write_inputStream() throws IOException {
    // Given
    var notifiedId = new AtomicReference<>();
    var path = randomPath();
    var output = new TaskOutput(blobId, "data", path, notifiedId::set);
    var data = randomBytes(1024);
    var in = new ByteArrayInputStream(data);

    // When
    output.write(in);

    // Then
    var written = Files.readAllBytes(path);
    assertThat(written).containsExactly(data);
    assertThat(notifiedId.get()).isEqualTo(blobId);
  }

  private static byte[] randomBytes(int size) {
    byte[] data = new byte[size];
    new Random(1234).nextBytes(data);
    return data;
  }

  private Path randomPath() throws IOException {
    return Files.createTempFile(tempDir, "output-", "");
  }
}
