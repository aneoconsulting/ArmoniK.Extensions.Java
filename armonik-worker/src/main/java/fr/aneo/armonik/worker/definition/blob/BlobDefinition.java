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
package fr.aneo.armonik.worker.definition.blob;

import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Definition of a blob to be created in ArmoniK's object storage.
 * <p>
 * A {@code BlobDefinition} describes how to create a blob that can be used as:
 * </p>
 * <ul>
 *   <li>Data dependency (input for a subtask)</li>
 *   <li>Expected output (result to be produced by a subtask)</li>
 * </ul>
 */
public sealed interface BlobDefinition permits InMemoryBlob, StreamBlob {

  /**
   * Returns the name of this blob.
   * <p>
   * This name is used as metadata in ArmoniK's object storage.
   * The server generates a unique ID regardless of the name.
   * An empty string indicates no explicit name was provided.
   * </p>
   *
   * @return the blob name; never {@code null}, may be empty
   */
  String name();

  /**
   * Returns an input stream to read the blob data.
   *
   * @return an input stream providing the blob data; never {@code null}
   */
  InputStream asStream();

  /**
   * Creates a blob definition from a byte array with an explicit name.
   * <p>
   * The byte array is kept in memory until the blob is created. This is suitable for
   * small to medium-sized data (typically &lt; 10 MB).
   * </p>
   *
   * @param name the blob name for this blob in ArmoniK; must not be {@code null}
   * @param data the blob data; must not be {@code null}
   * @return a blob definition wrapping the byte array; never {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   */
  static BlobDefinition from(String name, byte[] data) {
    requireNonNull(data, "data cannot be null");
    requireNonNull(name, "name cannot be null");

    return new InMemoryBlob(name, data);
  }

  /**
   * Creates a blob definition from a byte array without an explicit name.
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
   * @return a blob definition wrapping the byte array; never {@code null}
   * @throws NullPointerException if {@code data} is {@code null}
   */
  static BlobDefinition from(byte[] data) {
    requireNonNull(data, "data cannot be null");

    return from("", data);
  }

  /**
   * Creates a blob definition from an input stream with an explicit name.
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
   *     BlobDefinition blob = BlobDefinition.from("data", stream);
   *     BlobHandle handle = context.createBlob(blob);
   *     // Stream is consumed at this point
   * } // Stream is closed here
   * }</pre>
   *
   * @param name   the blob name for this blob in ArmoniK; must not be {@code null}
   * @param stream the input stream providing blob data; must not be {@code null}
   * @return a blob definition wrapping the input stream; never {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   */
  static BlobDefinition from(String name, InputStream stream) {
    requireNonNull(name, "name cannot be null");
    requireNonNull(stream, "stream cannot be null");
    return new StreamBlob(name, stream);
  }

  /**
   * Creates a blob definition from an input stream without an explicit name.
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
   * @return a blob definition wrapping the input stream; never {@code null}
   * @throws NullPointerException if {@code stream} is {@code null}
   */
  static BlobDefinition from(InputStream stream) {
    return from("", stream);
  }


}
