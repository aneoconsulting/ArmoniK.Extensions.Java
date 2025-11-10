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
package fr.aneo.armonik.worker.domain.internal;

import fr.aneo.armonik.worker.domain.ArmoniKException;
import fr.aneo.armonik.worker.domain.BlobId;
import fr.aneo.armonik.worker.domain.TaskOutput;

import java.io.InputStream;

/**
 * Abstraction for writing blob data to storage.
 * <p>
 * Implementations of this interface handle the actual storage mechanism
 * (files, network streams, object storage, etc.) and any required notifications
 * when data has been successfully written.
 * </p>
 *
 * <h2>Implementation Requirements</h2>
 * <p>
 * Implementations must:
 * </p>
 * <ul>
 *   <li>Write the complete data provided</li>
 *   <li>Ensure data is durably stored before returning</li>
 *   <li>Throw {@link ArmoniKException} if writing fails</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Implementations should be thread-safe to allow concurrent writes to different
 * blob IDs. Whether concurrent writes to the same blob ID are supported is
 * implementation-specific.
 * </p>
 *
 * <h2>Stream Handling</h2>
 * <p>
 * When writing from an {@link InputStream}, implementations must:
 * </p>
 * <ul>
 *   <li>Fully consume the stream</li>
 *   <li>NOT close the stream (caller's responsibility)</li>
 *   <li>Handle I/O errors by wrapping in {@link ArmoniKException}</li>
 * </ul>
 *
 * @see BlobId
 * @see TaskOutput
 * @see ArmoniKException
 */
public interface BlobWriter {

  /**
   * Writes blob data from a byte array.
   * <p>
   * The data is written to storage and the implementation ensures that any
   * required notifications (e.g., to the ArmoniK Agent) are sent upon
   * successful completion.
   * </p>
   *
   * @param id   the blob identifier; must not be {@code null}
   * @param data the data to write; must not be {@code null}
   * @throws ArmoniKException     if the write operation fails for any reason
   * @throws NullPointerException if any parameter is {@code null}
   */
  void write(BlobId id, byte[] data);

  /**
   * Writes blob data from an input stream.
   * <p>
   * The stream is fully consumed but NOT closed by this method. The caller
   * is responsible for closing the stream, typically using try-with-resources:
   * </p>
   * <pre>{@code
   * try (InputStream in = openDataSource()) {
   *     blobWriter.write(blobId, in);
   * }
   * }</pre>
   * <p>
   * The data is written to storage and the implementation ensures that any
   * required notifications (e.g., to the ArmoniK Agent) are sent upon
   * successful completion.
   * </p>
   *
   * @param id          the blob identifier; must not be {@code null}
   * @param inputStream the input stream providing blob data; must not be {@code null}
   * @throws ArmoniKException     if the write operation fails for any reason
   * @throws NullPointerException if any parameter is {@code null}
   */
  void write(BlobId id, InputStream inputStream);
}
