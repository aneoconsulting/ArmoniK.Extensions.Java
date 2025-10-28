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

import fr.aneo.armonik.client.definition.TaskDefinition;

import static java.util.Objects.requireNonNull;


/**
 * Definition of inline data content to be stored as a blob in the ArmoniK cluster.
 * <p>
 * A {@code BlobDefinition} encapsulates raw byte data that can be associated with task
 * inputs or used for blob creation. It represents data that will be uploaded to the
 * ArmoniK cluster.
 * <p>
 * This class is immutable and thread-safe, making it suitable for concurrent use
 * across multiple task definitions or blob operations.
 *
 * @see TaskDefinition#withInput(String, BlobDefinition)
 */
public class BlobDefinition {
  private final BlobData data;

  private BlobDefinition(BlobData data) {
    this.data = data;
  }

  /**
   * Returns the raw data content of this blob definition.
   * <p>
   * The returned byte array contains the actual data that will be uploaded to
   * the ArmoniK cluster when this definition is used in blob operations.
   * <p>
   * <strong>Note:</strong> The returned array is the internal array
   * used by this definition. <strong>Do not modify the returned array</strong> as it may affect
   * other operations using this definition. This is especially critical during upload operations
   * where the array is accessed directly using zero-copy optimizations - modifications during
   * upload can cause data corruption.
   * <p>
   * <strong>Thread Safety:</strong> While this class is thread-safe for read operations,
   * concurrent modification of the underlying data array is not safe and can lead to
   * unpredictable behavior during upload operations.
   *
   * @return the data content as a byte array, never null
   */
  public BlobData data() {
    return data;
  }

  /**
   * Creates a blob definition from the specified data content.
   * <p>
   * This factory method wraps the provided byte array in a blob definition
   * suitable for use in task input specifications or direct blob uploads.
   * <p>
   * <strong>Note:</strong> No defensive copy of the provided array is made. The array is used directly by
   * the definition and <strong>should not be modified after passing it to this method</strong>. The array will
   * be accessed directly during upload operations using zero-copy optimizations for performance.
   * Any modifications to the array after creating this definition can lead to data
   * corruption during upload.
   *
   * @param data the data content to wrap in a blob definition
   * @return a new blob definition containing the specified data
   * @throws NullPointerException if data is null
   * @see TaskDefinition#withInput(String, BlobDefinition)
   */
  public static BlobDefinition from(byte[] data) {
    requireNonNull(data, "data must not be null");

    return new BlobDefinition(InMemoryBlobData.from(data));
  }
}
