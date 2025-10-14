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
package fr.aneo.armonik.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.*;

/**
 * Provides write-only access to task output data stored in the ArmoniK shared data folder.
 * <p>
 * A {@code TaskOutput} represents a single output blob that a task must produce, identified by
 * a logical name. It provides multiple ways to write output data and automatically notifies
 * the ArmoniK Agent when the output is ready.
 * </p>
 *
 * <h2>Write Operations</h2>
 * <p>
 * Task outputs support three write methods depending on the data source:
 * </p>
 * <ul>
 *   <li><strong>Byte array</strong>: {@link #write(byte[])} - for in-memory data</li>
 *   <li><strong>Input stream</strong>: {@link #write(InputStream)} - for streaming or large data</li>
 *   <li><strong>String</strong>: {@link #write(String, Charset)} - for text data with custom encoding</li>
 * </ul>
 * <p>
 * All write methods automatically create or truncate the output file and notify the Agent
 * when the write completes successfully.
 * </p>
 *
 * <h2>Agent Notification</h2>
 * <p>
 * After each successful write operation:
 * </p>
 * <ol>
 *   <li>The output data is written to the file in the shared data folder</li>
 *   <li>The {@link BlobListener} is notified with the output's {@link BlobId}</li>
 *   <li>The Agent receives the notification and marks the output as available</li>
 *   <li>Dependent tasks waiting for this output can now be scheduled</li>
 * </ol>
 * <p>
 * This notification mechanism is critical for ArmoniK's task dependency management and
 * dynamic graph execution.
 * </p>
 *
 * <h2>File Behavior</h2>
 * <ul>
 *   <li>Each write operation creates the file if it doesn't exist</li>
 *   <li>Existing files are truncated before writing (previous content is lost)</li>
 *   <li>Multiple writes to the same output replace the previous content</li>
 *   <li>The file is closed automatically after each write</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * // Write computed result as bytes
 * TaskOutput resultOutput = taskHandler.getOutput("result");
 * byte[] resultData = computeResult();
 * resultOutput.write(resultData);
 *
 * // Write text output with UTF-8 encoding
 * TaskOutput logOutput = taskHandler.getOutput("log");
 * logOutput.write("Task completed successfully at " + Instant.now(), StandardCharsets.UTF_8);
 *
 * // Write text output with specific encoding
 * TaskOutput legacyOutput = taskHandler.getOutput("legacyReport");
 * legacyOutput.write("Report content", StandardCharsets.ISO_8859_1);
 *
 * // Stream large output
 * TaskOutput dataOutput = taskHandler.getOutput("processedData");
 * try (InputStream in = generateLargeDataset()) {
 *     dataOutput.write(in);
 * }
 *
 * // Conditional output
 * if (taskHandler.hasOutput("errorReport")) {
 *     taskHandler.getOutput("errorReport")
 *         .write(generateErrorReport(), StandardCharsets.UTF_8);
 * }
 * }</pre>
 *
 * <h2>Error Handling</h2>
 * <p>
 * All write methods throw {@link ArmoniKException} if:
 * </p>
 * <ul>
 *   <li>The output file cannot be created or written (I/O errors)</li>
 *   <li>Permissions prevent file creation or modification</li>
 *   <li>The file system is full or unavailable</li>
 *   <li>The input stream throws an exception during reading</li>
 * </ul>
 * <p>
 * When a write fails, the Agent is not notified, and the output is not marked as available.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * {@code TaskOutput} is not thread-safe. Concurrent writes to the same output may result in
 * corrupted data or multiple Agent notifications. Since the Worker processes one task at a
 * time, concurrent access is not expected during normal operation.
 * </p>
 *
 * <h2>Output Delegation</h2>
 * <p>
 * A task can delegate the responsibility of producing an output to a subtask by submitting
 * a new task with one or more of the current task's expected outputs. This enables dynamic
 * task graphs where the graph structure evolves during execution.
 * </p>
 *
 * @see TaskHandler
 * @see TaskInput
 * @see TaskProcessor
 * @see BlobListener
 */
public final class TaskOutput {
  private static final Logger logger = LoggerFactory.getLogger(TaskOutput.class);

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

  /**
   * Writes the provided byte array to the output file.
   * <p>
   * This method creates or truncates the output file and writes the entire byte array.
   * After successful completion, the Agent is notified that this output is ready.
   * </p>
   *
   * <h4>File Operations</h4>
   * <ul>
   *   <li>Creates the file if it doesn't exist</li>
   *   <li>Truncates existing content before writing</li>
   *   <li>Writes all bytes in a single atomic operation</li>
   *   <li>Closes the file automatically</li>
   * </ul>
   *
   * <h4>Performance Considerations</h4>
   * <p>
   * This method loads the entire byte array into memory during the write operation.
   * For very large data, consider using {@link #write(InputStream)} to stream the data
   * and reduce memory pressure.
   * </p>
   *
   * @param data the byte array to write; must not be {@code null}
   * @throws ArmoniKException if the file cannot be written due to I/O errors
   */
  public void write(byte[] data) {
    long startTime = System.nanoTime();

    try {
      logger.info("Writing output: logicalName='{}', blobId={}, size={} bytes", logicalName, id.asString(), data.length);
      Files.write(path, data, CREATE, TRUNCATE_EXISTING, WRITE);

      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      logger.info("Output written in {}ms: logicalName='{}', size={} bytes", duration, logicalName, data.length);

      listener.onBlobReady(id);
    } catch (IOException e) {
      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      logger.error("Failed to write output after {}ms: logicalName='{}', blobId={}", duration, logicalName, id.asString(), e);

      throw new ArmoniKException("Failed to write output " + id + "('" + logicalName + "') to " + path, e);
    }
  }

  /**
   * Writes data from the provided input stream to the output file.
   * <p>
   * This method creates or truncates the output file and streams all data from the input
   * stream until EOF is reached. After successful completion, the Agent is notified that
   * this output is ready.
   * </p>
   *
   * <h4>Streaming Behavior</h4>
   * <ul>
   *   <li>Reads from the input stream in chunks</li>
   *   <li>Writes data progressively to the output file</li>
   *   <li>Suitable for large data that should not be loaded entirely into memory</li>
   *   <li>The input stream is fully consumed but not closed by this method</li>
   * </ul>
   *
   * <h4>Resource Management</h4>
   * <p>
   * The caller is responsible for closing the input stream. Use try-with-resources:
   * </p>
   * <pre>{@code
   * try (InputStream in = openDataSource()) {
   *     taskOutput.write(in);
   * }
   * }</pre>
   *
   * @param in the input stream to read from; must not be {@code null}
   * @throws ArmoniKException if the file cannot be written or if reading from the
   *                          input stream fails
   */
  public void write(InputStream in) {
    long startTime = System.nanoTime();
    logger.debug("Writing output from stream: logicalName='{}', blobId={}", logicalName, id.asString());

    try (var out = Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING, WRITE)) {
      long bytesWritten = in.transferTo(out);
      listener.onBlobReady(id);

      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      logger.info("Output written from stream in {}ms: logicalName='{}', size={} bytes", duration, logicalName, bytesWritten);

    } catch (IOException e) {
      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      logger.error("Failed to write output stream after {}ms: logicalName='{}', blobId={}", duration, logicalName, id.asString(), e);

      throw new ArmoniKException("Failed to write output " + id + "('" + logicalName + "') to " + path, e);
    }
  }

  /**
   * Writes the provided string to the output file using the specified character encoding.
   * <p>
   * This method encodes the string using the provided charset and writes the resulting bytes
   * to the output file. After successful completion, the Agent is notified that this output is ready.
   * </p>
   *
   * <h4>Implementation Note</h4>
   * <p>
   * This method is a convenience wrapper that encodes the string and delegates to
   * {@link #write(byte[])}. The entire string is encoded into memory before writing.
   * </p>
   *
   * @param content the string content to write; must not be {@code null}
   * @param charset the character encoding to use for encoding the string
   * @throws ArmoniKException if the file cannot be written due to I/O errors
   * @see StandardCharsets
   */
  public void write(String content, Charset charset) {
    write(content.getBytes(charset));
  }
}
