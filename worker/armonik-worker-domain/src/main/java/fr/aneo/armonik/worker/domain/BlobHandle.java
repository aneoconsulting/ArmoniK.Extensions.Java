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

import fr.aneo.armonik.worker.domain.internal.BlobService;

import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

/**
 * Handle to a blob in the ArmoniK cluster.
 * <p>
 * A blob handle provides asynchronous access to blob metadata and represents
 * a reference to data stored in the ArmoniK cluster's object storage. Blob
 * handles are immutable references that do not directly contain the blob data,
 * but provide access to metadata including the blob's unique identifier.
 * </p>
 *
 * <h2>Usage Context</h2>
 * <p>
 * <strong>Production:</strong> Blob handles are created by the worker infrastructure
 * through {@link BlobService} operations (e.g., {@code createBlob()}, {@code prepareBlobs()}).
 * Applications receive handles when creating input blobs or preparing output blobs,
 * and through {@link TaskHandle} for task inputs and outputs.
 * </p>
 *
 * <h2>Asynchronous Metadata Access</h2>
 * <p>
 * The blob's metadata ({@link BlobInfo}) is provided asynchronously through
 * {@link #deferredBlobInfo()}. This completion stage completes when the ArmoniK
 * cluster acknowledges the blob creation and assigns the blob identifier.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * {@code BlobHandle} instances are immutable and thread-safe. Multiple threads
 * can safely access the same handle concurrently.
 * </p>
 *
 * @see BlobInfo
 * @see BlobId
 * @see BlobService
 * @see TaskHandle
 */
public final class BlobHandle {
  private final SessionId sessionId;
  private final String name;
  private final CompletionStage<BlobInfo> deferredBlobInfo;

  /**
   * Creates a blob handle with the specified properties.
   * <p>
   * <strong>Usage Context:</strong> In production environments, blob handles are
   * typically created by the worker infrastructure through {@link BlobService}
   * operations. This public constructor is primarily provided for testing purposes
   * when implementing custom task processors.
   * </p>
   *
   * @param sessionId        the session containing this blob; must not be {@code null}
   * @param name             the blob's internal name as defined by the blob definition;
   *                         may be {@code null}
   * @param deferredBlobInfo completion stage that will provide blob metadata when
   *                         the cluster acknowledges blob creation; must not be {@code null}
   * @throws NullPointerException if {@code sessionId} or {@code deferredBlobInfo} is {@code null}
   * @see BlobService#createBlob(fr.aneo.armonik.worker.domain.definition.blob.InputBlobDefinition)
   * @see BlobService#prepareBlobs(java.util.Map)
   */
  public BlobHandle(SessionId sessionId, String name, CompletionStage<BlobInfo> deferredBlobInfo) {
    this.sessionId = requireNonNull(sessionId, "sessionId cannot be null");
    this.name = name;
    this.deferredBlobInfo = requireNonNull(deferredBlobInfo, "deferredBlobInfo cannot be null");
  }

  /**
   * Returns the identifier of the session containing this blob.
   * <p>
   * All blobs belong to a session context that defines their lifecycle
   * and access scope.
   * </p>
   *
   * @return the session identifier; never {@code null}
   * @see SessionId
   */
  public SessionId sessionId() {
    return sessionId;
  }

  /**
   * Returns the blob's internal name, if specified.
   * <p>
   * The name is defined by the {@link fr.aneo.armonik.worker.domain.definition.blob.BlobDefinition}
   * when the blob is created. This is distinct from the logical name used when
   * referencing blobs in task definitions (which is the map key in
   * {@link TaskHandle#inputs()} or {@link TaskHandle#outputs()}).
   * </p>
   * <p>
   * The name may be {@code null} if not specified in the blob definition.
   * </p>
   *
   * @return the blob name, or {@code null} if not specified
   */
  public String name() {
    return name;
  }

  /**
   * Returns a completion stage that provides immutable metadata about this blob.
   * <p>
   * The blob information becomes available after the ArmoniK cluster acknowledges
   * the blob creation and assigns the blob identifier. The returned completion stage
   * completes normally with {@link BlobInfo} when metadata is available, or completes
   * exceptionally if blob creation or metadata retrieval fails.
   * </p>
   * <p>
   * This method provides non-blocking access to blob metadata, including the
   * blob's unique identifier, status, and creation timestamp.
   * </p>
   *
   * <h4>Usage Example</h4>
   * <pre>{@code
   * BlobHandle handle = blobService.createBlob(definition);
   *
   * // Non-blocking access to blob ID
   * handle.deferredBlobInfo()
   *       .thenAccept(info -> {
   *         BlobId id = info.id();
   *         System.out.println("Blob created with ID: " + id);
   *       });
   * }</pre>
   *
   * @return a completion stage that completes with blob metadata; never {@code null}
   * @see BlobInfo
   */
  public CompletionStage<BlobInfo> deferredBlobInfo() {
    return deferredBlobInfo;
  }

  @Override
  public String toString() {
    return "BlobHandle[sessionId=" + sessionId + ", name='" + name + "']";
  }
}
