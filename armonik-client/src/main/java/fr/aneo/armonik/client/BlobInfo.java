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
package fr.aneo.armonik.client;

import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.client.definition.blob.OutputBlobDefinition;

import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * Immutable metadata describing a blob within the ArmoniK distributed computing platform.
 * <p>
 * This class encapsulates the essential information about a blob after it has been
 * acknowledged and assigned identifiers by the ArmoniK cluster. The blob information
 * becomes available through {@link BlobHandle#deferredBlobInfo()} once the cluster
 * processes the blob creation request.
 * <p>
 * <strong>Key Properties:</strong>
 * <ul>
 *   <li><strong>ID:</strong> Unique cluster-assigned identifier for blob access and referencing</li>
 *   <li><strong>Session:</strong> The session context in which the blob was created</li>
 *   <li><strong>Name:</strong> Optional human-readable name for monitoring and debugging</li>
 *   <li><strong>Deletion Policy:</strong> Whether the blob requires manual deletion or is automatically cleaned up</li>
 *   <li><strong>Creation Metadata:</strong> Information about when and by which task the blob was created</li>
 * </ul>
 * <p>
 * <strong>Blob Lifecycle:</strong>
 * <ol>
 *   <li>Blob is defined via {@link InputBlobDefinition} or {@link OutputBlobDefinition}</li>
 *   <li>Cluster allocates the blob and assigns a unique ID</li>
 *   <li>BlobInfo becomes available through deferred completion</li>
 *   <li>Blob can be accessed for upload/download operations</li>
 *   <li>Blob is cleaned up based on deletion policy</li>
 * </ol>
 * <p>
 * <strong>Equality Semantics:</strong>
 * <p>
 * Two {@code BlobInfo} instances are considered equal if they have the same blob ID,
 * regardless of other metadata differences. This reflects the fact that the blob ID
 * uniquely identifies a blob within the ArmoniK cluster.
 *
 * @see BlobHandle#deferredBlobInfo()
 * @see BlobId
 * @see SessionHandle#createBlob(InputBlobDefinition)
 */
public final class BlobInfo {
  private final BlobId id;
  private final SessionId sessionId;
  private final String name;
  private final boolean manualDeletion;
  private final TaskId createdBy;
  private final Instant createdAt;

  /**
   * Constructs a new blob information record with complete metadata.
   * <p>
   * This constructor is package-private and intended for internal use by the ArmoniK client
   * infrastructure. Client applications should obtain {@code BlobInfo} instances through
   * {@link BlobHandle#deferredBlobInfo()} rather than constructing them directly.
   *
   * @param id             the unique identifier assigned to the blob by the ArmoniK cluster; must not be null
   * @param sessionId      the session in which the blob was created; must not be null
   * @param name           the human-readable name for the blob; defaults to empty string if null
   * @param manualDeletion {@code true} if the blob requires manual deletion; {@code false} for automatic cleanup
   * @param createdBy      the task that created this blob, or {@code null} if created directly by the client
   * @param createdAt      the timestamp when the blob was created, or {@code null} if not available
   * @throws NullPointerException if {@code id} or {@code sessionId} is null
   */
   BlobInfo(BlobId id, SessionId sessionId, String name, boolean manualDeletion, TaskId createdBy, Instant createdAt) {
     this.id = requireNonNull(id, "id must not be null");
     this.sessionId = requireNonNull(sessionId, "sessionId must not be null");
     this.name = requireNonNullElse(name, "");
     this.manualDeletion = manualDeletion;
     this.createdBy = createdBy;
     this.createdAt = createdAt;
   }

  /**
   * Returns the unique identifier of this blob within the ArmoniK cluster.
   * <p>
   * The blob ID is assigned by the cluster during blob creation and serves as the
   * primary reference for all blob operations including data upload, download, and
   * task dependency declarations.
   *
   * @return the blob identifier; never null
   * @see BlobHandle
   */
  public BlobId id() {
    return id;
  }

  /**
   * Returns the identifier of the session in which this blob was created.
   * <p>
   * All blobs belong to a specific session context, which defines their visibility
   * and lifecycle scope. Blobs are typically accessible only within their owning session
   * and are cleaned up when the session is closed (unless configured for manual deletion).
   *
   * @return the session identifier; never null
   * @see SessionHandle
   * @see SessionId
   */
  public SessionId sessionId() {
     return sessionId;
  }

  /**
   * Returns the human-readable name assigned to this blob.
   * <p>
   * The blob name is optional metadata used for monitoring, debugging, and cluster
   * administration purposes. It is independent of the parameter names used in task
   * definitions and provides a way to identify blobs in the ArmoniK monitoring interfaces.
   * <p>
   * If no name was specified during blob creation, this method returns an empty string.
   *
   * @return the blob name; never null but may be empty
   * @see InputBlobDefinition#withName(String)
   * @see OutputBlobDefinition#from(String, boolean)
   */
  public String name() {
     return name;
  }

  /**
   * Returns whether this blob requires manual deletion.
   * <p>
   * <strong>Deletion Policies:</strong>
   * <ul>
   *   <li><strong>Automatic (false):</strong> The blob is automatically cleaned up when
   *       the session is closed or when all dependent tasks complete</li>
   *   <li><strong>Manual (true):</strong> The blob persists beyond session lifetime and
   *       requires explicit deletion through cluster management interfaces</li>
   * </ul>
   *
   * @return {@code true} if manual deletion is required; {@code false} for automatic cleanup
   * @see InputBlobDefinition#withManualDeletion(boolean)
   * @see OutputBlobDefinition#from(String, boolean)
   */
  public boolean manualDeletion() {
     return manualDeletion;
  }

  /**
   * Returns the identifier of the task that created this blob, if any.
   * <p>
   * This field is populated when the blob is created as a task output. For blobs
   * created directly by the client through {@link SessionHandle#createBlob(InputBlobDefinition)},
   * this method returns {@code null}.
   *
   * @return the task ID of the creator task, or {@code null} if created by the client
   * @see TaskHandle
   * @see TaskId
   */
  public TaskId createdBy() {
     return createdBy;
  }

  /**
   * Returns the timestamp when this blob was created in the ArmoniK cluster.
   * <p>
   * This timestamp is assigned by the cluster and reflects when the blob metadata
   * was registered, not necessarily when data upload completed.
   *
   * @return the creation timestamp, or {@code null} if not available
   */
  public Instant createdAt() {
     return createdAt;
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
    return "BlobInfo[" +
      "id=" + id + ']';
  }
}
