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
package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.worker.ArmoniKException;
import fr.aneo.armonik.worker.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class for validating file paths and preventing directory traversal attacks.
 * <p>
 * {@code PathValidator} is used internally by {@link TaskContext}
 * to ensure that all file operations remain within the designated data folder. It prevents
 * malicious or accidental access to files outside the Worker's sandbox.
 * </p>
 *
 * <h2>Security</h2>
 * <p>
 * This class implements critical security checks to prevent:
 * </p>
 * <ul>
 *   <li><strong>Directory traversal attacks</strong>: Prevents paths like "../../../etc/passwd"</li>
 *   <li><strong>Subdirectory access</strong>: Only allows files at the root of the data folder</li>
 *   <li><strong>Symbolic link exploitation</strong>: Normalizes paths before validation</li>
 * </ul>
 *
 * <h2>Path Resolution Rules</h2>
 * <p>
 * When resolving a child path within a root:
 * </p>
 * <ol>
 *   <li>Both root and resolved paths are normalized to eliminate ".." and "." segments</li>
 *   <li>The resolved path must start with the root path (no escaping allowed)</li>
 *   <li>The resolved path's parent must equal the root (no subdirectories allowed)</li>
 * </ol>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is stateless and thread-safe. All methods are static.
 * </p>
 *
 * @see fr.aneo.armonik.worker.TaskInput
 * @see fr.aneo.armonik.worker.TaskOutput
 */
public final class PathValidator {

  private static final Logger logger = LoggerFactory.getLogger(PathValidator.class);

  private PathValidator() {
  }

  /**
   * Validates that a path points to an existing file (not a directory).
   * <p>
   * This method performs two checks:
   * </p>
   * <ol>
   *   <li>The path exists in the file system</li>
   *   <li>The path points to a regular file, not a directory</li>
   * </ol>
   * <p>
   * This validation is used to ensure that input files exist before attempting to read them.
   * </p>
   *
   * @param path the path to validate; must not be {@code null}
   * @throws ArmoniKException     if the path does not exist or is a directory
   * @throws NullPointerException if {@code path} is {@code null}
   */
  public static void validateFile(Path path) {
    if (!Files.exists(path)) {
      logger.error("File does not exist: {}", path);
      throw new ArmoniKException("File " + path + " does not exist");
    }
    if (Files.isDirectory(path)) {
      logger.error("Path is a directory, not a file: {}", path);
      throw new ArmoniKException("Path " + path + " is a directory");
    }
  }

  /**
   * Resolves a child path within a root directory, ensuring it remains within bounds.
   * <p>
   * This method safely resolves a relative path against a root directory while enforcing
   * two critical security constraints:
   * </p>
   * <ol>
   *   <li>The resolved path must remain within the root directory (no directory traversal)</li>
   *   <li>The resolved file must be at the root level (no subdirectories allowed)</li>
   * </ol>
   *
   * <h4>Path Normalization</h4>
   * <p>
   * Both the root and resolved paths are normalized before comparison, which eliminates:
   * </p>
   * <ul>
   *   <li>Redundant path segments (e.g., "/a/./b" → "/a/b")</li>
   *   <li>Parent directory references (e.g., "/a/b/../c" → "/a/c")</li>
   *   <li>Trailing slashes</li>
   * </ul>
   *
   * <h4>Security Checks</h4>
   * <p>
   * The method performs two security validations:
   * </p>
   * <ul>
   *   <li><strong>Boundary check</strong>: Ensures the resolved path starts with the root path</li>
   *   <li><strong>Depth check</strong>: Ensures the file is directly under the root (no subdirectories)</li>
   * </ul>
   *
   * <h4>Valid Examples</h4>
   * <pre>{@code
   * resolveWithin(Path.of("/data"), "file.txt")        → /data/file.txt ✓
   * resolveWithin(Path.of("/data"), "result-123.bin")  → /data/result-123.bin ✓
   * resolveWithin(Path.of("/data"), "./file.txt")      → /data/file.txt ✓
   * }</pre>
   *
   * <h4>Invalid Examples</h4>
   * <pre>{@code
   * resolveWithin(Path.of("/data"), "../config")       → throws (escapes root)
   * resolveWithin(Path.of("/data"), "sub/file.txt")    → throws (subdirectory)
   * resolveWithin(Path.of("/data"), "a/../../../etc")  → throws (escapes root)
   * }</pre>
   *
   * @param root  the root directory path; must not be {@code null}
   * @param child the relative path to resolve within the root; must not be {@code null}
   * @return the validated resolved path, guaranteed to be within the root and at root level
   * @throws ArmoniKException     if the child resolves outside the root directory or is not
   *                              at the root level
   * @throws NullPointerException if either parameter is {@code null}
   */
  public static Path resolveWithin(Path root, String child) {
    var normalizedRoot = root.normalize();
    var resolved = normalizedRoot.resolve(child).normalize();

    if (!resolved.startsWith(normalizedRoot)) {
      logger.error("Security violation: Path '{}' resolves outside data folder. Root: {}, Resolved: {}", child, normalizedRoot, resolved);

      throw new ArmoniKException(
        child + " resolves outside data folder. Expected: " + normalizedRoot + ", Got: " + resolved
      );
    }

    if (!resolved.getParent().equals(normalizedRoot)) {
      logger.error("Security violation: Path '{}' attempts subdirectory access. Root: {}, Resolved: {}", child, normalizedRoot, resolved);
      throw new ArmoniKException(
        "Only files at the root of dataFolder are allowed: '" + child + "' is not valid"
      );
    }

    return resolved;
  }
}
