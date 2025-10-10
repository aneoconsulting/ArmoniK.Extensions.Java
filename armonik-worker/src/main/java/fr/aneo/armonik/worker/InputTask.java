package fr.aneo.armonik.worker;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;

public final class InputTask {
  static final long CACHE_THRESHOLD_BYTES = 8L * 1024 * 1024; // 8 MiB

  private final Path path;
  private final String logicalName;
  private byte[] cache;

  InputTask(String logicalName, Path path) {
    this.path = path;
    this.logicalName = logicalName;
  }

  public long size() {
    try {
      return Files.size(path);
    } catch (IOException e) {
      throw new ArmoniKException("Failed to get size of input '" + logicalName + "' from " + path, e);
    }
  }

  public byte[] rawData() {
    byte[] local = cache;
    if (local != null) return local;

    try {
      long len = Files.size(path);
      byte[] bytes = Files.readAllBytes(path);
      if (len <= CACHE_THRESHOLD_BYTES) {
        cache = bytes;
      }
      return bytes;
    } catch (IOException e) {
      throw new ArmoniKException("Failed to read input '" + logicalName + "' from " + path, e);
    }
  }

  public InputStream stream() throws IOException {
    return Files.newInputStream(path, READ);
  }


  public String asString() {
    return new String(rawData(), UTF_8);
  }


  public long asLong() {
    return Long.parseLong(asString().trim());
  }


  public double asDouble() {
    return Double.parseDouble(asString().trim());
  }

  public BigDecimal asBigDecimal() {
    return new BigDecimal(asString().trim());
  }

  public BigInteger asBigInteger() {
    return new BigInteger(asString().trim());
  }

  public boolean asBoolean() {
    return Boolean.parseBoolean(asString().trim());
  }
}
