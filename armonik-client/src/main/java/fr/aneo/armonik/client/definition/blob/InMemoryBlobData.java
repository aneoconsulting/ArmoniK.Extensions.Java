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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Represents in-memory data content as a byte array.
 * <p>
 * {@code InMemoryBlobData} encapsulates raw byte array data that is already
 * loaded in memory. This is suitable for small to medium-sized data that can
 * comfortably fit in memory.
 * <p>
 * <strong>Memory Considerations:</strong>
 * <ul>
 *   <li>The entire data array is kept in memory during upload</li>
 *   <li>Best suited for data up to a few megabytes</li>
 *   <li>For larger data, consider {@link FileBlobData}</li>
 * </ul>
 * <p>
 * <strong>Data Integrity:</strong> The provided byte array is used directly without
 * defensive copying for performance reasons. <strong>Do not modify the array</strong>
 * after passing it to this class, as modifications during upload can cause data
 * corruption.
 * <p>
 * This class is immutable and thread-safe for read operations, but the underlying
 * byte array must not be modified concurrently.
 *
 * @see BlobData
 * @see FileBlobData
 */
public final class InMemoryBlobData implements BlobData {
  private final byte[] data;

  /**
   * Creates in-memory blob data from the specified byte array.
   * <p>
   * <strong>Important:</strong> No defensive copy of the data array is made. The array
   * is used directly and <strong>must not be modified</strong> after creation.
   *
   * @param data the data content as a byte array
   * @throws NullPointerException if data is null
   */
  private InMemoryBlobData(byte[] data) {
    this.data = requireNonNull(data, "data must not be null");
  }

  /**
   * Returns the raw data content.
   * <p>
   * <strong>Warning:</strong> The returned byte array is the internal array used by
   * this instance. <strong>Do not modify the returned array</strong> as it may affect
   * upload operations.
   *
   * @return the data content as a byte array, never null
   */
  public byte[] data() {
    return data;
  }

  @Override
  public InputStream stream() {
    return new ByteArrayInputStream(data);
  }

  @Override
  public boolean isRetryable() {
    return true;
  }

  /**
   * Creates in-memory blob data from the specified byte array.
   * <p>
   * <strong>Note:</strong> No defensive copy of the data array is made. The array is used
   * directly and <strong>must not be modified</strong> after passing it to this method.
   *
   * @param data the data content as a byte array
   * @return a new in-memory blob data instance
   * @throws NullPointerException if data is null
   */
  public static InMemoryBlobData from(byte[] data) {
    return new InMemoryBlobData(data);
  }
}
