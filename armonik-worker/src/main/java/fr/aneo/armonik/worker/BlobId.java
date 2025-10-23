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
package fr.aneo.armonik.worker;

import java.util.Objects;

/**
 * Immutable identifier for a blob (binary large object) in the ArmoniK system.
 * <p>
 * A {@code BlobId} represents a unique identifier for task inputs and outputs stored in the
 * ArmoniK object storage. Blobs are referenced by their IDs throughout the task execution
 * lifecycle, from submission to result retrieval.
 * </p>
 *
 * <h2>Purpose</h2>
 * <p>
 * Blob IDs serve multiple purposes in ArmoniK:
 * </p>
 * <ul>
 *   <li><strong>Identification</strong>: Uniquely identify results in the Control Plane</li>
 *   <li><strong>File naming</strong>: Determine the filename in the shared data folder</li>
 *   <li><strong>Agent notification</strong>: Notify the Agent when outputs are ready</li>
 *   <li><strong>Dependency tracking</strong>: Link tasks through their input/output relationships</li>
 * </ul>
 *
 * <h2>Value Semantics</h2>
 * <p>
 * {@code BlobId} is a value object with proper equality semantics:
 * </p>
 * <ul>
 *   <li>Two blob IDs are equal if they contain the same string identifier</li>
 *   <li>Hash code is based on the identifier string</li>
 *   <li>Instances are immutable and thread-safe</li>
 * </ul>
 *
 * @see TaskInput
 * @see TaskOutput
 * @see BlobListener
 */
public final class BlobId {
  private final String id;

  private BlobId(String id) {
    this.id = id;
  }

  /**
   * Returns the string representation of this blob ID.
   * <p>
   * This is the actual identifier used in the ArmoniK system, typically a UUID or
   * result ID assigned by the Control Plane.
   * </p>
   *
   * @return the blob identifier as a string; never {@code null}
   */
  public String asString() {
    return id;
  }

  /**
   * Creates a blob ID from a string identifier.
   * <p>
   * This is the factory method used internally by {@link TaskHandler} when parsing
   * the payload and creating {@link TaskInput} and {@link TaskOutput} instances.
   * </p>
   *
   * @param id the string identifier; must not be {@code null}
   * @return a new blob ID wrapping the identifier; never {@code null}
   * @throws NullPointerException if {@code id} is {@code null}
   */
  static BlobId from(String id) {
    return new BlobId(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (BlobId) obj;
    return Objects.equals(this.id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return "BlobId[" +
      "id=" + id + ']';
  }
}
