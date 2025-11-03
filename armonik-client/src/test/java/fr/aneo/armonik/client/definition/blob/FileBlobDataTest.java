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

import fr.aneo.armonik.client.exception.ArmoniKException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class FileBlobDataTest {

  @Test
  @DisplayName("should create file blob data from file")
  void should_create_file_blob_data_from_file(@TempDir Path tempDir) throws IOException {
    // Given
    var file = tempDir.resolve("test.bin").toFile();
    var data = "test data".getBytes();
    Files.write(file.toPath(), data);

    // When
    var blobData = FileBlobData.from(file);

    // Then
    assertThat(blobData).isNotNull();
    assertThat(blobData.file()).isEqualTo(file);
  }

  @Test
  @DisplayName("should create file blob data from file path")
  void should_create_file_blob_data_from_file_path(@TempDir Path tempDir) throws IOException {
    // Given
    var file = tempDir.resolve("test.bin");
    var data = "test data".getBytes();
    Files.write(file, data);

    // When
    var blobData = FileBlobData.from(file.toString());

    // Then
    assertThat(blobData).isNotNull();
    assertThat(blobData.file().getAbsolutePath()).isEqualTo(file.toAbsolutePath().toString());
  }

  @Test
  @DisplayName("should throw null pointer exception when file is null")
  void should_throw_null_pointer_exception_when_file_is_null() {
    // When / Then
    assertThatThrownBy(() -> FileBlobData.from((File) null))
      .isInstanceOf(NullPointerException.class)
      .hasMessageContaining("file must not be null");
  }

  @Test
  @DisplayName("should throw null pointer exception when file path is null")
  void should_throw_null_pointer_exception_when_file_path_is_null() {
    // When / Then
    assertThatThrownBy(() -> FileBlobData.from((String) null))
      .isInstanceOf(NullPointerException.class)
      .hasMessageContaining("filePath must not be null");
  }

  @Test
  @DisplayName("should throw armonik exception when file does not exist")
  void should_throw_armonik_exception_when_file_does_not_exist() {
    // Given
    var nonExistentFile = new File("/path/to/nonexistent/file.bin");

    // When / Then
    assertThatThrownBy(() -> FileBlobData.from(nonExistentFile))
      .isInstanceOf(ArmoniKException.class)
      .hasRootCauseInstanceOf(IOException.class);
  }

  @Test
  @DisplayName("should throw armonik exception when file path does not exist")
  void should_throw_armonik_exception_when_file_path_does_not_exist() {
    // Given
    var nonExistentPath = "/path/to/nonexistent/file.bin";

    // When / Then
    assertThatThrownBy(() -> FileBlobData.from(nonExistentPath))
      .isInstanceOf(ArmoniKException.class)
      .hasRootCauseInstanceOf(IOException.class);
  }

  @Test
  @DisplayName("should return readable stream")
  void should_return_readable_stream(@TempDir Path tempDir) throws IOException {
    // Given
    var file = tempDir.resolve("data.bin");
    var data = "stream test".getBytes();
    Files.write(file, data);
    var blobData = FileBlobData.from(file.toFile());

    // When
    try (var stream = blobData.stream()) {
      var readData = stream.readAllBytes();

      // Then
      assertThat(readData).isEqualTo(data);
    }
  }

  @Test
  @DisplayName("should return independent streams on multiple calls")
  void should_return_independent_streams_on_multiple_calls(@TempDir Path tempDir) throws IOException {
    // Given
    var file = tempDir.resolve("multi-stream.bin");
    var data = "independent streams".getBytes();
    Files.write(file, data);
    var blobData = FileBlobData.from(file.toString());

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
