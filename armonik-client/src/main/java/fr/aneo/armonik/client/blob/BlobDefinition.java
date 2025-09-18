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
package fr.aneo.armonik.client.blob;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;


/**
 * Definition of an inline blob payload to be created in ArmoniK.
 * <p>
 * A {@code BlobDefinition} encapsulates the raw bytes of a payload that can be associated
 * with a logical name when defining a task input. It is typically consumed by blob-related
 * services when creating input blobs prior to task submission.
 * </p>
 */
public class BlobDefinition {
  private final byte[] data;

  private BlobDefinition(byte[] data) {
    this.data = data;
  }

  /**
   * Returns the raw payload bytes.
   *
   * @return the payload bytes (never {@code null})
   */
  public byte[] data() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;

    BlobDefinition that = (BlobDefinition) o;
    return Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }

  /**
   * Creates a BlobDefinition from the given payload.
   *
   * @param data the payload bytes; must not be {@code null}
   * @return a new blob definition wrapping the given payload
   * @throws NullPointerException if {@code data} is {@code null}
   */
  public static BlobDefinition from(byte[] data) {
    requireNonNull(data, "data must not be null");

    return new BlobDefinition(data);
  }
}
