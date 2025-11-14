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

import fr.aneo.armonik.worker.domain.internal.BlobWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * Provides write-only access to task output data.
 * <p>
 * A {@code TaskOutput} represents a single output that a task must produce, identified by
 * a logical name. It provides methods to write output data which automatically notifies
 * the ArmoniK Agent when the output is ready.
 * </p>
 *
 * <h2>Write Operations</h2>
 * <p>
 * Task outputs support three write methods:
 * </p>
 * <ul>
 *   <li>{@link #write(byte[])} - for in-memory data</li>
 *   <li>{@link #write(InputStream)} - for streaming or large data</li>
 *   <li>{@link #write(String, Charset)} - for text data with custom encoding</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * // Write bytes
 * TaskOutput output = context.getOutput("result");
 * output.write(computeResult());
 *
 * // Write text
 * output.write("Task completed", StandardCharsets.UTF_8);
 *
 * // Stream large data
 * try (InputStream in = generateData()) {
 *     output.write(in);
 * }
 * }</pre>
 *
 * <h2>Output Delegation</h2>
 * <p>
 * A task can delegate output production to a subtask by including the output
 * in the subtask's expected outputs. This enables dynamic task graphs.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is not thread-safe. Concurrent writes may result in corrupted data.
 * </p>
 *
 * @see TaskContext
 * @see TaskInput
 * @see TaskProcessor
 */
public final class TaskOutput {
  private static final Logger logger = LoggerFactory.getLogger(TaskOutput.class);

  private final BlobId id;
  private final String logicalName;
  private final BlobWriter writer;

  public TaskOutput(BlobId id, String logicalName, BlobWriter writer) {
    this.id = requireNonNull(id, "id cannot be null");
    this.logicalName = requireNonNull(logicalName, "logicalName cannot be null");
    this.writer = requireNonNull(writer, "writer cannot be null");
  }

  public BlobId id() {
    return id;
  }

  /**
   * Writes a byte array to this output.
   * <p>
   * After successful completion, the Agent is notified that this output is ready.
   * For very large data, consider using {@link #write(InputStream)} instead.
   * </p>
   *
   * @param data the data to write; must not be {@code null}
   * @throws ArmoniKException     if the write operation fails
   * @throws NullPointerException if {@code data} is {@code null}
   */
  public void write(byte[] data) {
    requireNonNull(data, "data cannot be null");

    logger.debug("Writing output: logicalName='{}', id={}, size={} bytes", logicalName, id.asString(), data.length);
    writer.write(id, data);
  }

  /**
   * Writes data from an input stream to this output.
   * <p>
   * The stream is fully consumed but not closed by this method.
   * After successful completion, the Agent is notified that this output is ready.
   * </p>
   * <p>
   * Use try-with-resources to ensure proper cleanup:
   * </p>
   * <pre>{@code
   * try (InputStream in = openDataSource()) {
   *     taskOutput.write(in);
   * }
   * }</pre>
   *
   * @param inputStream the input stream to read from; must not be {@code null}
   * @throws ArmoniKException     if the write operation fails
   * @throws NullPointerException if {@code in} is {@code null}
   */
  public void write(InputStream inputStream) {
    requireNonNull(inputStream, "input stream cannot be null");

    logger.debug("Writing output from stream: logicalName='{}', id={}", logicalName, id.asString());
    writer.write(id, inputStream);
  }

  /**
   * Writes a string to this output using the specified character encoding.
   * <p>
   * The string is encoded to bytes and written. After successful completion,
   * the Agent is notified that this output is ready.
   * </p>
   *
   * @param content the string to write; must not be {@code null}
   * @param charset the character encoding to use; must not be {@code null}
   * @throws ArmoniKException     if the write operation fails
   * @throws NullPointerException if any parameter is {@code null}
   * @see StandardCharsets
   */
  public void write(String content, Charset charset) {
    requireNonNull(content, "content cannot be null");
    requireNonNull(charset, "charset cannot be null");

    logger.debug("Writing output from string: logicalName='{}', id={}, charset={}", logicalName, id.asString(), charset.name());
    write(content.getBytes(charset));
  }
}
