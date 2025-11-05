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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Input blob definition backed by an in-memory byte array.
 * <p>
 * The data is already loaded in memory and will be written directly to the shared folder
 * when the blob is created.
 * </p>
 * <p>
 * This implementation is suitable for small to medium-sized data (typically &lt; 10 MB).
 * For larger datasets, consider using {@link StreamBlobDefinition} instead.
 * </p>
 *
 * @see InputBlobDefinition
 * @see StreamBlobDefinition
 */
public final class InMemoryBlobDefinition implements InputBlobDefinition {
  private final String name;
  private final byte[] data;

  /**
   * Package-private constructor to enforce factory method usage.
   *
   * @param name the blob name; must not be {@code null}
   * @param data the blob data; must not be {@code null}
   */
  InMemoryBlobDefinition(String name, byte[] data) {
    this.data = requireNonNull(data, "data cannot be null");
    this.name = requireNonNull(name, "name cannot be null");
  }

  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the byte array containing the blob data.
   * <p>
   * This method provides direct access to the internal byte array for optimization
   * purposes (e.g., inline upload without stream overhead).
   * </p>
   *
   * @return the blob data; never {@code null}
   */
  public byte[] data() {
    return data;
  }

  @Override
  public InputStream asStream() {
    return new ByteArrayInputStream(data);
  }

  @Override
  public String toString() {
    return "InMemoryBlob[name='" + name + "', size=" + data.length + " bytes]";
  }
}
