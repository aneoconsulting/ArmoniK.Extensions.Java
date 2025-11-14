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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static fr.aneo.armonik.worker.domain.TaskInput.CACHE_THRESHOLD_BYTES;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TaskInputTest {

  @TempDir
  private Path tempDir;
  private final BlobId blobId = BlobId.from("id");

  @Test
  @DisplayName("size returns file size in bytes")
  void size_returns_file_size_in_bytes() throws IOException {
    // Given
    var file = write(randomBytes(6));
    var input = new TaskInput(blobId, "input", file);

    // When
    var size = input.size();

    // Then
    assertThat(size).isEqualTo(6);
  }

  @Test
  @DisplayName("rawData caches bytes for files up to 8 MiB inclusive")
  void rawData_caches_bytes_for_files_up_to_8_mib_inclusive() throws IOException {
    // Given
    var data = randomBytes((int) CACHE_THRESHOLD_BYTES);
    var file = write(data);
    var input = new TaskInput(blobId, "input", file);

    // When
    var first = input.rawData();
    var second = input.rawData();

    // Then
    assertThat(second).isSameAs(first);
    assertThat(first).containsExactly(data);
  }

  @Test
  @DisplayName("rawData does not cache for files larger than 8 MiB")
  void rawData_does_not_cache_for_files_larger_than_8_mib() throws IOException {
    // Given
    var data = randomBytes((int) CACHE_THRESHOLD_BYTES + 1);
    var file = write(data);
    var input = new TaskInput(blobId, "input", file);

    // When
    var first = input.rawData();
    var second = input.rawData();

    // Then
    assertThat(second).isNotSameAs(first);
    assertThat(first).containsExactly(data);
    assertThat(second).containsExactly(data);
  }

  @Test
  @DisplayName("stream provides fresh readable InputStream each call")
  void stream_provides_fresh_readable_InputStream_each_call() throws IOException {
    // Given
    var file = writeString("abc");
    var input = new TaskInput(blobId, "input", file);

    // WHen
    try (var in1 = input.stream(); var in2 = input.stream()) {
      var bytes1 = in1.readAllBytes();
      var bytes2 = in2.readAllBytes();

      // Then
      assertThat(new String(bytes1)).isEqualTo("abc");
      assertThat(new String(bytes2)).isEqualTo("abc");
    }
  }

  @Test
  @DisplayName("asString returns UTF-8 decoded content")
  void asString_returns_utf8_decoded_content() throws IOException {
    // Given
    var file = writeString("héllo 🌍");
    var input = new TaskInput(blobId, "input", file);

    // When
    var string = input.asString(UTF_8);

    // Then
    assertThat(string).isEqualTo("héllo 🌍");
  }

  private static byte[] randomBytes(int size) {
    byte[] data = new byte[size];
    new Random(1234).nextBytes(data);
    return data;
  }

  private Path write(byte[] bytes) throws IOException {
    var path = Files.createTempFile(tempDir, "input-", "");
    Files.write(path, bytes);
    return path;
  }

  private Path writeString(String content) throws IOException {
    var path = Files.createTempFile(tempDir, "input-","");
    Files.writeString(path, content);
    return path;
  }
}
