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
package fr.aneo.armonik.worker.domain.definition.blob;

import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Definition of an input blob containing data to be uploaded to ArmoniK's object storage.
 * <p>
 * An {@code InputBlobDefinition} represents a blob that includes both metadata (name) and
 * data content. These blobs are used as:
 * </p>
 * <ul>
 *   <li>Data dependencies for tasks (input data)</li>
 *   <li>Task payloads</li>
 * </ul>
 * <p>
 * Two implementations are provided:
 * </p>
 * <ul>
 *   <li>{@link InMemoryBlobDefinition} - for data already loaded in memory (byte arrays)</li>
 *   <li>{@link StreamBlobDefinition} - for streaming large datasets from input streams</li>
 * </ul>
 *
 * @see BlobDefinition
 * @see InMemoryBlobDefinition
 * @see StreamBlobDefinition
 */
public sealed interface InputBlobDefinition extends BlobDefinition permits InMemoryBlobDefinition, StreamBlobDefinition {

  /**
   * Returns an input stream to read the blob data.
   * <p>
   * This method provides access to the blob's content for upload to ArmoniK's
   * object storage.
   * </p>
   *
   * @return an input stream providing the blob data; never {@code null}
   */
  InputStream asStream();

  /**
   * Creates an input blob definition from a byte array with an explicit name.
   * <p>
   * The byte array is kept in memory until the blob is created. This is suitable for
   * small to medium-sized data (typically &lt; 10 MB).
   * </p>
   *
   * @param name the blob name for this blob in ArmoniK; must not be {@code null}
   * @param data the blob data; must not be {@code null}
   * @return an input blob definition wrapping the byte array; never {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   */
  static InputBlobDefinition from(String name, byte[] data) {
    requireNonNull(data, "data cannot be null");
    requireNonNull(name, "name cannot be null");

    return new InMemoryBlobDefinition(name, data);
  }

  /**
   * Creates an input blob definition from a byte array without an explicit name.
   * <p>
   * The blob name will be set to an empty string. The ArmoniK server will generate
   * a unique ID for the blob regardless of the name.
   * </p>
   * <p>
   * The byte array is kept in memory until the blob is created. This is suitable for
   * small to medium-sized data (typically &lt; 10 MB).
   * </p>
   *
   * @param data the blob data; must not be {@code null}
   * @return an input blob definition wrapping the byte array; never {@code null}
   * @throws NullPointerException if {@code data} is {@code null}
   */
  static InputBlobDefinition from(byte[] data) {
    requireNonNull(data, "data cannot be null");

    return from("", data);
  }

  /**
   * Creates an input blob definition from an input stream with an explicit name.
   * <p>
   * <strong>Stream Lifecycle:</strong> The caller is responsible for managing the stream's
   * lifecycle.
   * </p>
   * <p>
   * <strong>Non-blocking:</strong> The stream is only consumed when the blob is created.
   * This allows for streaming large datasets without loading them entirely into memory.
   * </p>
   * <p>
   * <strong>Resource Management:</strong> Use try-with-resources to ensure proper cleanup:
   * </p>
   * <pre>{@code
   * try (InputStream stream = openDataSource()) {
   *     InputBlobDefinition blob = InputBlobDefinition.from("data", stream);
   *     BlobHandle handle = context.createBlob(blob);
   *     // Stream is consumed at this point
   * } // Stream is closed here
   * }</pre>
   *
   * @param name   the blob name for this blob in ArmoniK; must not be {@code null}
   * @param stream the input stream providing blob data; must not be {@code null}
   * @return an input blob definition wrapping the input stream; never {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   */
  static InputBlobDefinition from(String name, InputStream stream) {
    requireNonNull(name, "name cannot be null");
    requireNonNull(stream, "stream cannot be null");
    return new StreamBlobDefinition(name, stream);
  }

  /**
   * Creates an input blob definition from an input stream without an explicit name.
   * <p>
   * The blob name will be set to an empty string. The ArmoniK server will generate
   * a unique ID for the blob regardless of the name.
   * </p>
   * <p>
   * <strong>Stream Lifecycle:</strong> The caller is responsible for managing the stream's
   * lifecycle. Use try-with-resources to ensure proper cleanup.
   * </p>
   *
   * @param stream the input stream providing blob data; must not be {@code null}
   * @return an input blob definition wrapping the input stream; never {@code null}
   * @throws NullPointerException if {@code stream} is {@code null}
   */
  static InputBlobDefinition from(InputStream stream) {
    return from("", stream);
  }
}
