package fr.aneo.armonik.worker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

public final class OutputTask {

  private final Path path;
  private final String logicalName;

  OutputTask(String logicalName, Path path) {
    this.path = path;
    this.logicalName = logicalName;
  }

  public void write(byte[] data) {
    try {
      Files.write(path, data, CREATE, TRUNCATE_EXISTING, WRITE);
    } catch (IOException e) {
      throw new ArmoniKException("Failed to write output '" + logicalName + "' to " + path, e);
    }
  }

  public void write(InputStream in) {
    try (var out = Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING, WRITE)) {
      in.transferTo(out);
    } catch (IOException e) {
      throw new ArmoniKException("Failed to stream-write output '" + logicalName + "' to " + path, e);
    }
  }

  public void writeString(String content) {
    write(content.getBytes(UTF_8));
  }
}
