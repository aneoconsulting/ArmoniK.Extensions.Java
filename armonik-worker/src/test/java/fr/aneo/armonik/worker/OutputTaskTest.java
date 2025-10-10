package fr.aneo.armonik.worker;


import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class OutputTaskTest {
  @TempDir
  private Path tempDir;

  @Test
  @DisplayName("write(byte[]) creates file and truncates on subsequent writes")
  void write_bytes_creates_and_truncates_on_subsequent_writes() throws IOException {
    // Given
    var path = randomPath();
    var output = new OutputTask("result", path);
    var first = randomBytes(32);
    var second = randomBytes(7);

    // When
    output.write(first);
    var afterFirst = Files.readAllBytes(path);

    output.write(second);
    var afterSecond = Files.readAllBytes(path);

    // Then
    assertThat(afterFirst).containsExactly(first);
    assertThat(afterSecond).containsExactly(second);
    assertThat(afterSecond.length).isEqualTo(7);
  }

  @Test
  @DisplayName("write(String) writes UTF-8 text")
  void write_writes_utf8_text() throws IOException {
    // Given
    var path = randomPath();
    var output = new OutputTask("greeting", path);
    var text = "héllo 🌍";

    // When
    output.write(text, UTF_8);

    // Then
    var content = Files.readString(path, UTF_8);
    assertThat(content).isEqualTo(text);
  }

  @Test
  @DisplayName("write(InputStream) copies all bytes from the stream")
  void write_stream_copies_all_bytes() throws IOException {
    // Given
    var path = randomPath();
    var output = new OutputTask("data", path);
    var data = randomBytes(1024);
    var in = new ByteArrayInputStream(data);

    // When
    output.write(in);

    // Then
    var written = Files.readAllBytes(path);
    assertThat(written).containsExactly(data);
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
