package fr.aneo.armonik.worker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;

public final class TaskOutput {

  private final BlobId id;
  private final Path path;
  private final String logicalName;
  private final BlobListener listener;

  TaskOutput(BlobId id, String logicalName, Path path, BlobListener listener) {
    this.id = id;
    this.logicalName = logicalName;
    this.path = path;
    this.listener = listener;
  }

  public void write(byte[] data) {
    try {
      Files.write(path, data, CREATE, TRUNCATE_EXISTING, WRITE);
      listener.onBlobReady(id);
    } catch (IOException e) {
      throw new ArmoniKException("Failed to write output " + id + "('" + logicalName + "') to " + path, e);
    }
  }

  public void write(InputStream in) {
    try (var out = Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING, WRITE)) {
      in.transferTo(out);
      listener.onBlobReady(id);
    } catch (IOException e) {
      throw new ArmoniKException("Failed to write output " + id + "('" + logicalName + "') to " + path, e);
    }
  }

  public void write(String content, Charset charset) {
    write(content.getBytes(charset));
  }
}
