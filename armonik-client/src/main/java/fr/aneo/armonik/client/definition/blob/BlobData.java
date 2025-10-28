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
package fr.aneo.armonik.client.definition.blob;

import java.io.InputStream;

/**
 * Represents the actual data content for blob upload operations.
 * <p>
 * {@code BlobData} abstracts different data sources (in-memory byte arrays, files,
 * custom streams) and provides a uniform interface for accessing the data during upload
 * to the ArmoniK cluster.
 * Implementations should be immutable and thread-safe.
 *
 * @see InMemoryBlobData
 * @see FileBlobData
 */
public abstract class BlobData {

  /**
   * Opens an input stream to read the data content.
   * <p>
   * The returned stream provides access to the raw data bytes that will be uploaded
   * to the ArmoniK cluster. Implementations must ensure the stream is readable
   * and positioned at the start of the data.
   * <p>
   * <strong>Caller Responsibility:</strong> The caller is responsible for closing
   * the returned stream after use to prevent resource leaks.
   * <p>
   * <strong>Thread Safety:</strong> Each invocation should return a fresh stream.
   * Multiple threads can call this method concurrently to get independent streams.
   *
   * @return a new input stream positioned at the start of the data
   * @throws java.io.IOException if an I/O error occurs opening the stream
   */
  public abstract InputStream stream() throws java.io.IOException;
}
