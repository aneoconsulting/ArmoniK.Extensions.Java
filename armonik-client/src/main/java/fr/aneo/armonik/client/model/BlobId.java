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
package fr.aneo.armonik.client.model;

import java.util.Objects;

/**
 * Immutable identifier for a blob within the ArmoniK distributed computing platform.
 * <p>
 * Blob identifiers are assigned by the ArmoniK cluster during blob creation and
 * uniquely identify blobs across the entire cluster.
 * <p>
 * BlobId instances are immutable and implement proper equality and hash code semantics
 * for use in collections and as map keys.
 *
 * @see BlobInfo
 * @see BlobHandle
 */
public class BlobId {
  private final String id;

  private BlobId(String id) {
    this.id = id;
  }

  /**
   * Returns the string representation of this blob identifier.
   *
   * @return the blob identifier as a string
   */
  public String asString() {
    return id;
  }

  /**
   * Creates a blob identifier from the given string.
   * <p>
   * This factory method creates blob identifiers from cluster-assigned string
   * identifiers received through the gRPC API.
   *
   * @param id the string identifier assigned by the ArmoniK cluster
   * @return a new BlobId instance wrapping the given identifier
   * @throws NullPointerException if id is null
   */
  static BlobId from(String id) {
    return new BlobId(id);
  }

  @Override
  public String toString() {
    return "BlobId{" +
      "id='" + id + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;

    BlobId blobId = (BlobId) o;
    return Objects.equals(id, blobId.id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }
}
