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
package fr.aneo.armonik.worker.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Provides read-only access to task input data stored in the ArmoniK shared data folder.
 * <p>
 * A {@code TaskInput} represents a single input blob for a task, identified by a logical name.
 * It provides access to the raw binary data and string conversion with custom character encoding,
 * with automatic caching for frequently accessed small files.
 * </p>
 *
 * <h2>Data Access Patterns</h2>
 * <p>
 * Task inputs can be read using the following methods:
 * </p>
 * <ul>
 *   <li><strong>Raw bytes</strong>: {@link #rawData()} - loads entire file into memory with caching</li>
 *   <li><strong>Stream</strong>: {@link #stream()} - for large files or streaming processing</li>
 *   <li><strong>String</strong>: {@link #asString(Charset)} - for text data with custom encoding</li>
 * </ul>
 *
 * <h2>Caching Strategy</h2>
 * <p>
 * To optimize performance for frequently accessed inputs:
 * </p>
 * <ul>
 *   <li>Files ≤ 8 MiB are automatically cached in memory after the first read via {@link #rawData()}</li>
 *   <li>Subsequent calls to {@link #rawData()} return the cached data without file I/O</li>
 *   <li>Files > 8 MiB are never cached to avoid memory pressure</li>
 *   <li>Streaming access via {@link #stream()} bypasses the cache entirely</li>
 * </ul>
 * <p>
 * The cache threshold is defined by {@link #CACHE_THRESHOLD_BYTES} (8 MiB).
 * </p>
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * // Read small text input with UTF-8 encoding
 * TaskInput configInput = taskContext.getInput("config");
 * String config = configInput.asString(StandardCharsets.UTF_8);
 *
 * // Read text input with specific encoding
 * TaskInput dataInput = taskContext.getInput("legacy_data");
 * String data = dataInput.asString(StandardCharsets.ISO_8859_1);
 *
 * // Read raw binary data
 * TaskInput binaryInput = taskContext.getInput("image");
 * byte[] imageData = binaryInput.rawData();
 *
 * // Stream large binary input
 * TaskInput dataInput = taskContext.getInput("largeDataset");
 * try (InputStream in = dataInput.stream()) {
 *     processStreamingData(in);
 * }
 *
 * // Check size before reading
 * TaskInput input = taskContext.getInput("data");
 * if (input.size() < 100_000_000) {
 *     byte[] data = input.rawData(); // Safe for files < 100 MB
 * } else {
 *     try (InputStream in = input.stream()) {
 *         // Process in chunks
 *     }
 * }
 * }</pre>
 *
 * <h2>Error Handling</h2>
 * <p>
 * Methods throw {@link ArmoniKException} when:
 * </p>
 * <ul>
 *   <li>The input file cannot be read (I/O errors)</li>
 *   <li>The file has been deleted or moved</li>
 *   <li>Permissions prevent file access</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * {@code TaskInput} is not thread-safe. The cache field is not synchronized, and concurrent
 * access may result in redundant file reads. Since the Worker processes one task at a time,
 * concurrent access is not expected during normal operation.
 * </p>
 *
 * <h2>Memory Considerations</h2>
 * <p>
 * When using {@link #rawData()} on large files:
 * </p>
 * <ul>
 *   <li>Files > 8 MiB are loaded into memory but not cached</li>
 *   <li>Very large files should use {@link #stream()} to avoid OutOfMemoryError</li>
 *   <li>Use {@link #size()} to check file size before deciding on read strategy</li>
 * </ul>
 *
 * @see TaskContext
 * @see TaskOutput
 * @see TaskProcessor
 */
public final class TaskInput {
  static final long CACHE_THRESHOLD_BYTES = 8L * 1024 * 1024; // 8 MiB

  private static final Logger logger = LoggerFactory.getLogger(TaskInput.class);

  private final BlobId id;
  private final Path path;
  private final String logicalName;
  private byte[] cache;

  public TaskInput(BlobId id, String logicalName, Path path) {
    this.id = requireNonNull(id, "id cannot be null");
    this.path = requireNonNull(path, "path cannot be null");
    this.logicalName = requireNonNull(logicalName, "logicalName cannot be null");
  }

  public BlobId id() {
    return id;
  }

  /**
   * Converts this task input to a blob handle.
   * <p>
   * This method creates a {@link BlobHandle} representing this input blob with a
   * {@link BlobStatus#CREATED} status. The returned handle can be used to reference
   * this input blob in subsequent operations, such as passing it as a dependency to
   * subtasks.
   * </p>
   * <p>
   * The blob information is immediately available (non-deferred) since the input
   * blob already exists and has been validated by the ArmoniK infrastructure.
   * </p>
   *
   * <h4>Common Use Cases</h4>
   * <ul>
   *   <li>Passing task inputs as dependencies to subtasks</li>
   *   <li>Reusing input blobs across multiple task submissions</li>
   *   <li>Building task graphs that reference existing input data</li>
   * </ul>
   *
   * @return a blob handle with {@link BlobStatus#CREATED} status; never {@code null}
   * @see BlobHandle
   * @see BlobStatus#CREATED
   */
  public BlobHandle asBlobHandle() {
    var blobInfo = new BlobInfo(id, BlobStatus.CREATED);
    return new BlobHandle(logicalName, completedFuture(blobInfo));
  }

  /**
   * Returns the size of the input file in bytes.
   * <p>
   * This method queries the file system for the current file size.
   * Use this method to decide between loading the entire file with {@link #rawData()}
   * or streaming it with {@link #stream()}.
   * </p>
   *
   * @return the file size in bytes; always ≥ 0
   * @throws ArmoniKException if the file size cannot be determined due to I/O errors
   *                          or if the file no longer exists
   */
  public long size() {
    try {
      return Files.size(path);
    } catch (IOException e) {
      logger.error("Failed to get size of input: logicalName='{}', blobId={}", logicalName, id.asString(), e);
      throw new ArmoniKException("Failed to get size of input " + id + "('" + logicalName + "')" + " from " + path, e);
    }
  }

  /**
   * Reads and returns the entire input file as a byte array.
   * <p>
   * This method loads the complete file contents into memory. For files ≤ {@link #CACHE_THRESHOLD_BYTES}
   * (8 MiB), the data is cached in memory and subsequent calls return the cached copy without
   * additional file I/O.
   * </p>
   *
   * <h4>Caching Behavior</h4>
   * <ul>
   *   <li>First call: Reads file, caches if ≤ 8 MiB, returns data</li>
   *   <li>Subsequent calls (if cached): Returns cached data immediately</li>
   *   <li>Large files (> 8 MiB): Reads file on every call, no caching</li>
   * </ul>
   *
   * <h4>Performance Considerations</h4>
   * <ul>
   *   <li>Optimal for small to medium files (&lt; 8 MiB)</li>
   *   <li>For large files, consider using {@link #stream()} to avoid loading entire file into memory</li>
   *   <li>Check {@link #size()} before calling to avoid OutOfMemoryError on very large files</li>
   * </ul>
   *
   * @return the complete file contents as a byte array; never {@code null}
   * @throws ArmoniKException if the file cannot be read due to I/O errors
   */
  public byte[] rawData() {
    byte[] local = cache;
    if (local != null) {
      logger.debug("Returning cached input: logicalName='{}', size={} bytes", logicalName, local.length);
      return local;
    }

    try {
      long len = Files.size(path);
      logger.info("Reading input: logicalName='{}', blobId={}, size={} bytes", logicalName, id.asString(), len);
      byte[] bytes = Files.readAllBytes(path);
      if (len <= CACHE_THRESHOLD_BYTES) {
        cache = bytes;
      }

      logger.info("Input read: logicalName='{}', size={} bytes", logicalName, bytes.length);
      return bytes;
    } catch (IOException e) {
      logger.error("Failed to read input: logicalName='{}', blobId={}", logicalName, id.asString(), e);
      throw new ArmoniKException("Failed to read input '" + id + "('" + logicalName + "')" + " from " + path, e);
    }
  }

  /**
   * Opens an input stream for reading the file contents.
   * <p>
   * This method provides streaming access to the input data, which is ideal for:
   * </p>
   * <ul>
   *   <li>Large files that should not be loaded entirely into memory</li>
   *   <li>Processing data in chunks or pipelines</li>
   *   <li>Situations where only part of the file is needed</li>
   * </ul>
   * <p>
   * The stream bypasses the internal cache and reads directly from the file system.
   * </p>
   *
   * <h4>Resource Management</h4>
   * <p>
   * The caller is responsible for closing the returned stream. Use try-with-resources:
   * </p>
   * <pre>{@code
   * try (InputStream in = taskInput.stream()) {
   *     // Process stream
   * }
   * }</pre>
   *
   * @return an input stream for reading the file; never {@code null}
   * @throws IOException if the stream cannot be opened due to I/O errors or if the file
   *                     does not exist
   */
  public InputStream stream() throws IOException {
    logger.info("Opening stream for input: logicalName='{}', blobId={}", logicalName, id.asString());
    return Files.newInputStream(path, READ);
  }

  /**
   * Reads the entire input file and converts it to a string using the specified character encoding.
   * <p>
   * This method is a convenience wrapper around {@link #rawData()} that decodes the raw bytes
   * using the provided charset. It benefits from the same caching behavior as {@link #rawData()}.
   * </p>
   *
   * <h4>Performance Considerations</h4>
   * <p>
   * Since this method calls {@link #rawData()}, it inherits the same caching behavior:
   * </p>
   * <ul>
   *   <li>Files ≤ 8 MiB are cached after the first read</li>
   *   <li>Subsequent calls reuse the cached data, only performing charset decoding</li>
   *   <li>For large files, consider streaming and decoding in chunks instead</li>
   * </ul>
   *
   * @param charset the character encoding to use for decoding the file contents
   * @return the file contents as a string; never {@code null}
   * @throws ArmoniKException if the file cannot be read due to I/O errors
   */
  public String asString(Charset charset) {
    return new String(rawData(), charset);
  }
}
