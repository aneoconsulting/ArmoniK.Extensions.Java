package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.worker.ArmoniKException;

import java.nio.file.Files;
import java.nio.file.Path;

public final class PathValidator {

  private PathValidator() {}

  public static void validateFile(Path path) {
    if (!Files.exists(path)) {
      throw new ArmoniKException("File " + path + " does not exist");
    }
    if (Files.isDirectory(path)) {
      throw new ArmoniKException("Path " + path + " is a directory");
    }
  }

  public static Path resolveWithin(Path root, String child) {
    var normalizedRoot = root.normalize();
    var resolved = normalizedRoot.resolve(child).normalize();

    if (!resolved.startsWith(normalizedRoot)) {
      throw new ArmoniKException(
        child + " resolves outside data folder. Expected: " + normalizedRoot + ", Got: " + resolved
      );
    }

    if (!resolved.getParent().equals(normalizedRoot)) {
      throw new ArmoniKException(
        "Only files at the root of dataFolder are allowed: '" + child + "' is not valid"
      );
    }

    return resolved;
  }
}
