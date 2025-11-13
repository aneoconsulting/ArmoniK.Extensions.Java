package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.worker.domain.ArmoniKException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ZipExtractorTest {

  @TempDir
  Path tempDir;

  @Test
  @DisplayName("should reject ZIP entry attempting directory traversal with dot dot")
  void should_reject_zip_entry_with_directory_traversal() throws IOException {
    // Given
    Path zipFile = createZipWithEntry("../../etc/passwd", "malicious content");

    // When/Then
    assertThatThrownBy(() -> ZipExtractor.extract(zipFile, tempDir))
      .isInstanceOf(ArmoniKException.class)
      .hasMessageContaining("Security violation")
      .hasMessageContaining("directory traversal");
  }

  @Test
  @DisplayName("should reject ZIP entry with absolute path")
  void should_reject_zip_entry_with_absolute_path() throws IOException {
    // Given
    Path zipFile = createZipWithEntry("/etc/passwd", "malicious content");

    // When/Then
    assertThatThrownBy(() -> ZipExtractor.extract(zipFile, tempDir))
      .isInstanceOf(ArmoniKException.class)
      .hasMessageContaining("Security violation")
      .hasMessageContaining("absolute path");
  }

  @Test
  @DisplayName("should extract valid ZIP with subdirectories")
  void should_extract_valid_zip_with_subdirectories() throws IOException {
    // Given
    Path zipFile = createZipWithEntries(
      "lib/processor.jar", "jar content",
      "config/app.properties", "config content",
      "README.md", "readme content"
    );

    // When
    Path result = ZipExtractor.extract(zipFile, tempDir);

    // Then
    assertThat(result).isEqualTo(tempDir);
    assertThat(tempDir.resolve("lib/processor.jar")).exists().isRegularFile();
    assertThat(tempDir.resolve("config/app.properties")).exists().isRegularFile();
    assertThat(tempDir.resolve("README.md")).exists().isRegularFile();

    assertThat(Files.readString(tempDir.resolve("lib/processor.jar"))).isEqualTo("jar content");
  }

  @Test
  @DisplayName("should reject ZIP bomb exceeding size limit")
  void should_reject_zip_bomb_exceeding_size_limit() throws IOException {
    // Given
    byte[] data = new byte[2 * 1024 * 1024];
    Path zipFile = createZipWithEntries(
      "file1.bin", data,
      "file2.bin", data
    );

    // When/Then: Should reject with 3 MB limit
    long testLimit = 3L * 1024 * 1024; // 3 MB for testing
    assertThatThrownBy(() -> ZipExtractor.extract(zipFile, tempDir, testLimit))
      .isInstanceOf(ArmoniKException.class)
      .hasMessageContaining("ZIP bomb detected");
  }

  @Test
  @DisplayName("should handle ZIP with directories")
  void should_handle_zip_with_directories() throws IOException {
    // Given
    Path zipFile = tempDir.resolve("test.zip");
    try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
      zos.putNextEntry(new ZipEntry("lib/"));
      zos.closeEntry();
      zos.putNextEntry(new ZipEntry("lib/processor.jar"));
      zos.write("content".getBytes());
      zos.closeEntry();
    }

    // When
    ZipExtractor.extract(zipFile, tempDir);

    // Then
    assertThat(tempDir.resolve("lib")).isDirectory();
    assertThat(tempDir.resolve("lib/processor.jar")).exists();
  }

  private Path createZipWithEntry(String entryName, String content) throws IOException {
    return createZipWithEntries(entryName, content.getBytes());
  }

  private Path createZipWithEntries(String... nameContentPairs) throws IOException {
    Object[] converted = new Object[nameContentPairs.length];
    for (int i = 0; i < nameContentPairs.length; i++) {
      converted[i] = (i % 2 == 0) ? nameContentPairs[i] : nameContentPairs[i].getBytes();
    }
    return createZipWithEntries(converted);
  }

  private Path createZipWithEntries(Object... nameContentPairs) throws IOException {
    if (nameContentPairs.length % 2 != 0) {
      throw new IllegalArgumentException("Must provide name-content pairs");
    }

    var zipFile = tempDir.resolve("test.zip");
    try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
      for (int i = 0; i < nameContentPairs.length; i += 2) {
        var name = (String) nameContentPairs[i];
        var content = (byte[]) nameContentPairs[i + 1];

        zos.putNextEntry(new ZipEntry(name));
        zos.write(content);
        zos.closeEntry();
      }
    }
    return zipFile;
  }
}
