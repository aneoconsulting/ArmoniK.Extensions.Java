package fr.aneo.armonik.worker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static fr.aneo.armonik.worker.InputTask.CACHE_THRESHOLD_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

class InputTaskTest {

  @TempDir
  private Path tempDir;

  @Test
  @DisplayName("size returns file size in bytes")
  void size_returns_file_size_in_bytes() throws IOException {
    // Given
    var file = write(randomBytes(6));
    var input = new InputTask("input", file);

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
    var input = new InputTask("input", file);

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
    var input = new InputTask("input", file);

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
    var input = new InputTask("input", file);

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
    var input = new InputTask("input", file);

    // When
    var string = input.asString();

    // Then
    assertThat(string).isEqualTo("héllo 🌍");
  }

  @Test
  @DisplayName("asLong parses trimmed integer content")
  void asLong_parses_trimmed_integer_content() throws IOException {
    // Given
    var file = writeString("  42 \n");
    var input = new InputTask("input", file);

    // When
    var aLong = input.asLong();

    // Then
    assertThat(aLong).isEqualTo(42L);
  }

  @Test
  @DisplayName("asDouble parses trimmed double content")
  void asDouble_parses_trimmed_double_content() throws IOException {
    // Given
    var file = writeString("  3.1415  ");
    var input = new InputTask("input", file);

    // When
    var aDouble = input.asDouble();

    // Then
    assertThat(aDouble).isEqualTo(3.1415);
  }

  @Test
  @DisplayName("asBigDecimal parses decimal content precisely")
  void asBigDecimal_parses_decimal_content_precisely() throws IOException {
    // Given
    var file = writeString("1234567890.123456789");
    var input = new InputTask("input", file);

    // When
    var bigDecimal = input.asBigDecimal();

    // Then
    assertThat(bigDecimal).isEqualByComparingTo(new BigDecimal("1234567890.123456789"));
  }

  @Test
  @DisplayName("asBigInteger parses integer content")
  void asBigInteger_parses_integer_content() throws IOException {
    // Given
    var file = writeString("  9876543210123456789\n");
    var input = new InputTask("input", file);

    // When
    var bigInteger = input.asBigInteger();

    // Then
    assertThat(bigInteger).isEqualTo(new BigInteger("9876543210123456789"));
  }

  @Test
  @DisplayName("asBoolean parses trimmed boolean content")
  void asBoolean_parses_trimmed_boolean_content() throws IOException {
    // Given
    var trueInput = new InputTask("true_input", writeString("  true  "));
    var falseInput = new InputTask("false_input", writeString("  false  "));

    // When
    var trueBoolean = trueInput.asBoolean();
    var falseBoolean = falseInput.asBoolean();

    // Then
    assertThat(trueBoolean).isTrue();
    assertThat(falseBoolean).isFalse();
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
