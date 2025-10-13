package fr.aneo.armonik.worker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;

public final class TaskInput {
  static final long CACHE_THRESHOLD_BYTES = 8L * 1024 * 1024; // 8 MiB

  private final BlobId id;
  private final Path path;
  private final String logicalName;
  private byte[] cache;

  TaskInput(BlobId id, String logicalName, Path path) {
    this.id = id;
    this.path = path;
    this.logicalName = logicalName;
  }

  public long size() {
    try {
      return Files.size(path);
    } catch (IOException e) {
      throw new ArmoniKException("Failed to get size of input " + id + "('" + logicalName + "')" + " from " + path, e);
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
      throw new ArmoniKException("Failed to read input '" + id + "('" + logicalName + "')" + " from " + path, e);
    }
  }

  public InputStream stream() throws IOException {
    return Files.newInputStream(path, READ);
  }


  public String asString(Charset charset) {
    return new String(rawData(), charset);
  }
}
