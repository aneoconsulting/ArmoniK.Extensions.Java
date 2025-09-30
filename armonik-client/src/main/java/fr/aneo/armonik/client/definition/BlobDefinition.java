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
package fr.aneo.armonik.client.definition;

import fr.aneo.armonik.client.model.BlobHandle;

import static java.util.Objects.requireNonNull;


/**
 * Definition of inline data content to be stored as a blob in the ArmoniK cluster.
 * <p>
 * A {@code BlobDefinition} encapsulates raw byte data that can be associated with task
 * inputs or used for blob creation. It represents data that will be uploaded to the
 * ArmoniK cluster and serves as the content source for {@link BlobHandle#uploadData(BlobDefinition)}.
 * <p>
 * This class is immutable and thread-safe, making it suitable for concurrent use
 * across multiple task definitions or blob operations.
 *
 * @see BlobHandle#uploadData(BlobDefinition)
 * @see TaskDefinition#withInput(String, BlobDefinition)
 */
public class BlobDefinition {
  private final byte[] data;

  private BlobDefinition(byte[] data) {
    this.data = data;
  }

  /**
   * Returns the raw data content of this blob definition.
   * <p>
   * The returned byte array contains the actual data that will be uploaded to
   * the ArmoniK cluster when this definition is used in blob operations.
   * <p>
   * <strong>Note:</strong> The returned array is the internal array used by this
   * definition. Callers should not modify the returned array as it may affect
   * other operations using this definition.
   *
   * @return the data content as a byte array, never null
   */
  public byte[] data() {
    return data;
  }

  /**
   * Creates a blob definition from the specified data content.
   * <p>
   * This factory method wraps the provided byte array in a blob definition
   * suitable for use in task input specifications or direct blob uploads.
   * The data array is stored internally and should not be modified after
   * passing it to this method.
   *
   * @param data the data content to wrap in a blob definition
   * @return a new blob definition containing the specified data
   * @throws NullPointerException if data is null
   * @see TaskDefinition#withInput(String, BlobDefinition)
   * @see BlobHandle#uploadData(BlobDefinition)
   */
  public static BlobDefinition from(byte[] data) {
    requireNonNull(data, "data must not be null");

    return new BlobDefinition(data);
  }
}
