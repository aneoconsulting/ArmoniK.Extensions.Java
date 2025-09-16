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

import fr.aneo.armonik.client.session.SessionHandle;

import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

/**
 * Lightweight reference to a blob within a given session.
 * <p>
 * A {@code BlobHandle} ties a blob to its owning {@link SessionHandle} and provides asynchronous
 * access to immutable blob metadata through {@link #metadata()}.
 * The handle does not contain the blob content and does not perform blocking I/O.
 * </p>
 *
 * <p>
 * The {@linkplain #metadata() metadata future} may complete exceptionally if the metadata
 * cannot be retrieved from underlying services.
 * </p>
 *
 * @see BlobMetadata
 * @see SessionHandle
 */

public final class BlobHandle {
  private final SessionHandle sessionHandle;
  private final CompletionStage<BlobMetadata> metadata;

  /**
   * Creates a handle for a blob in a given session, with deferred metadata resolution.
   *
   * @param sessionHandle  session that owns or scopes the blob
   * @param metadata future providing the blob's metadata when available
   * @throws NullPointerException if {@code session} or {@code metadata} is {@code null}
   */
  BlobHandle(SessionHandle sessionHandle, CompletionStage<BlobMetadata> metadata) {
    requireNonNull(sessionHandle, "sessionHandle must not be null");
    requireNonNull(metadata, "blobMetaData must not be null");

    this.sessionHandle = sessionHandle;
    this.metadata = metadata;
  }

  /**
   * Returns the session handle that owns or scopes this blob.
   *
   * @return the owning session
   */
  public SessionHandle sessionHandle() {
    return sessionHandle;
  }

  /**
   * Returns a {@link CompletionStage} that completes with the immutable metadata of this blob.
   * <p>
   * The returned CompletionStage may complete exceptionally if the metadata cannot be retrieved.
   * </p>
   *
   * @return a {@link CompletionStage} of the blob metadata
   */
  public CompletionStage<BlobMetadata> metadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "BlobHandle[" +
      "sessionHandle=" + sessionHandle + "]";
  }
}
