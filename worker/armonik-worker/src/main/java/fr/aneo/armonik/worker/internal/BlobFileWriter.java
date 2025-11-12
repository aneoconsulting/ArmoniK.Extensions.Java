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

import fr.aneo.armonik.worker.domain.ArmoniKException;
import fr.aneo.armonik.worker.domain.BlobId;
import fr.aneo.armonik.worker.domain.internal.BlobWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.*;
import static java.util.Objects.requireNonNull;

/**
 * Writes blob data to files in the shared folder and notifies the Agent when complete.
 * <p>
 * This writer is responsible for:
 * </p>
 * <ol>
 *   <li>Writing blob data to a file named after the blob ID</li>
 *   <li>Validating the file path to prevent directory traversal attacks</li>
 *   <li>Notifying the Agent via {@link BlobListener} when the file is ready</li>
 * </ol>
 *
 * <h2>File Naming</h2>
 * <p>
 * Files are created in the shared folder with the blob ID as the filename:
 * {@code <dataFolder>/<blobId>}
 * </p>
 *
 * <h2>Error Handling</h2>
 * <p>
 * If writing fails, an {@link ArmoniKException} is thrown and the Agent is not notified.
 * This ensures the Agent doesn't mark incomplete blobs as ready.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe. Multiple writes can be performed concurrently to different
 * blob IDs.
 * </p>
 *
 * @see BlobListener
 */
public final class BlobFileWriter implements BlobWriter {

  private static final Logger logger = LoggerFactory.getLogger(BlobFileWriter.class);

  private final Path folderPath;
  private final BlobListener listener;

  BlobFileWriter(Path folderPath, BlobListener listener) {
    this.folderPath = requireNonNull(folderPath, "folderPath cannot be null");
    this.listener = requireNonNull(listener, "listener cannot be null");
  }

  /**
   * Writes a byte array to a file and notifies the Agent.
   * <p>
   * The file is created at {@code <folderPath>/<blobId>}. If the file already exists,
   * it is truncated before writing.
   * </p>
   *
   * @param id   the blob ID (used as filename); must not be {@code null}
   * @param data the blob data to write; must not be {@code null}
   * @throws ArmoniKException     if the file cannot be written or path validation fails
   * @throws NullPointerException if any parameter is {@code null}
   */
  @Override
  public void write(BlobId id, byte[] data) {
    requireNonNull(id, "id cannot be null");
    requireNonNull(data, "data cannot be null");

    var path = PathValidator.resolveWithin(folderPath, id.asString());
    long startTime = System.nanoTime();
    try {
      logger.debug("Writing blob: id={}, size={} bytes", id.asString(), data.length);
      Files.write(path, data, CREATE, TRUNCATE_EXISTING, WRITE);

      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      logger.info("Blob written in {}ms: id='{}', size={} bytes", duration, id.asString(), data.length);

      listener.onBlobReady(id);
    } catch (IOException e) {
      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      logger.error("Failed to write Blob after {}ms: id={}", duration, id.asString(), e);

      throw new ArmoniKException("Failed to write blob, id= '" + id + "' to " + path, e);
    }
  }

  /**
   * Writes an input stream to a file and notifies the Agent.
   * <p>
   * The file is created at {@code <folderPath>/<blobId>}. If the file already exists,
   * it is truncated before writing. The input stream is fully consumed but not closed
   * (the caller is responsible for closing it).
   * </p>
   *
   * @param id          the blob ID (used as filename); must not be {@code null}
   * @param inputStream the input stream providing blob data; must not be {@code null}
   * @throws ArmoniKException     if the file cannot be written or path validation fails
   * @throws NullPointerException if any parameter is {@code null}
   */
  @Override
  public void write(BlobId id, InputStream inputStream) {
    requireNonNull(id, "id cannot be null");
    requireNonNull(inputStream, "inputStream cannot be null");

    var path = PathValidator.resolveWithin(folderPath, id.asString());
    long startTime = System.nanoTime();
    logger.debug("Writing blob from stream: id={}", id.asString());

    try (var outputStream = Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING, WRITE)) {
      var bytesWritten = inputStream.transferTo(outputStream);

      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      logger.info("Blob written from stream in {}ms: id='{}', size={} bytes", duration, id.asString(), bytesWritten);

      listener.onBlobReady(id);
    } catch (IOException e) {
      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      logger.error("Failed to write output stream after {}ms: id={}", duration, id.asString(), e);

      throw new ArmoniKException("Failed to write blob, id= '" + id + "' to " + path, e);
    }
  }
}
