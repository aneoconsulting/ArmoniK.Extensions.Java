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
package fr.aneo.armonik.worker.domain;

import java.util.Objects;

/**
 * Immutable metadata about a blob in the ArmoniK cluster.
 * <p>
 * This class represents blob information as provided by the ArmoniK cluster,
 * including the blob's unique identifier, current status.
 * Instances are typically created by the worker infrastructure when blobs are
 * created or retrieved from the cluster.
 * </p>
 *
 * <h2>Usage Context</h2>
 * <p>
 * <strong>Production:</strong> Blob information is created by the worker infrastructure
 * based on cluster responses (via gRPC). Applications receive {@link BlobInfo} instances
 * through {@link BlobHandle#deferredBlobInfo()}.
 *  </p>
 * <h2>Equality</h2>
 * <p>
 * Two {@code BlobInfo} instances are considered equal if they have the same blob ID,
 * regardless of status. This reflects the fact that a blob's identity
 * is determined solely by its ID.
 * </p>
 *
 * @see BlobHandle
 * @see BlobId
 * @see BlobStatus
 */
public final class BlobInfo {
  private final BlobId id;
  private final BlobStatus status;

  /**
   * Creates blob information with the specified metadata.
   * <p>
   * <strong>Usage Context:</strong> In production environments, blob information is
   * typically created by the worker infrastructure based on cluster responses. This
   * public constructor is primarily provided for testing purposes when implementing
   * custom task processors.
   * </p>
   *
   * @param id     the blob identifier assigned by the cluster; must not be {@code null}
   * @param status the current status of the blob; must not be {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   */
  public BlobInfo(BlobId id, BlobStatus status) {
    this.id = Objects.requireNonNull(id, "id cannot be null");
    this.status = Objects.requireNonNull(status, "status cannot be null");
  }

  /**
   * Returns the unique identifier of this blob.
   *
   * @return the blob identifier; never {@code null}
   */
  public BlobId id() {
    return id;
  }

  /**
   * Returns the current status of this blob.
   *
   * @return the blob status; never {@code null}
   */
  public BlobStatus status() {
    return status;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (BlobInfo) obj;
    return Objects.equals(this.id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return "BlobInfo[id=" + id + ", status=" + status + "]";
  }
}
