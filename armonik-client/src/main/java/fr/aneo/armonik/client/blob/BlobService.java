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

import fr.aneo.armonik.client.session.Session;
import fr.aneo.armonik.client.task.TaskDefinition;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Service API for creating and referencing blobs (task inputs and outputs) in ArmoniK.
 * <p>
 * This facade allows callers to:
 * </p>
 * <ul>
 *   <li>pre-create metadata entries for expected result blobs,</li>
 *   <li>create input blobs from inline payload definitions, and</li>
 *   <li>download the content of a blob by its identifier.</li>
 * </ul>
 *
 * @see BlobHandle
 * @see BlobMetadata
 * @see BlobDefinition
 * @see Session
 * @see TaskDefinition
 */
public interface BlobService {

  /**
   * Creates metadata entries for a given number of output blobs in the provided session.
   * <p>
   * No binary content is uploaded by this operation; it only registers placeholders that can be
   * produced by subsequent task executions. The returned handles expose metadata asynchronously.
   * </p>
   *
   * @param session the session that will own the blobs; must not be {@code null}
   * @param count   the number of blob metadata entries to create; must be {@code >= 0}
   * @return a list of blob handles (possibly empty when {@code count == 0})
   * @throws NullPointerException     if {@code session} is {@code null}
   * @throws IllegalArgumentException if {@code count} is negative
   */
  List<BlobHandle> createBlobMetaData(Session session, int count);

  /**
   * Creates blobs in the provided session from the given payload definitions.
   * <p>
   * Each {@link BlobDefinition} provides the raw bytes for one blob. The position in the input list
   * maps to the corresponding handle in the returned list.
   * </p>
   *
   * @param session         the session that will own the created input blobs; must not be {@code null}
   * @param blobDefinitions the ordered list of payload definitions; must not be {@code null}
   * @return an ordered list of handles for the created blobs (same cardinality as {@code blobDefinitions})
   * @throws NullPointerException if {@code session} or {@code blobDefinitions} is {@code null}
   */
  List<BlobHandle> createBlobs(Session session, List<BlobDefinition> blobDefinitions);

  /**
   * Downloads the full content of a blob by its identifier.
   * <p>
   * The result completes asynchronously with the blob bytes or exceptionally if the download fails.
   * </p>
   *
   * @param session the session owning the blob; must not be {@code null}
   * @param blobId  the blob identifier; must not be {@code null}
   * @return a future that completes with the blob data
   * @throws NullPointerException if {@code session} or {@code blobId} is {@code null}
   */
  CompletionStage<byte[]> downloadBlob(Session session, UUID blobId);
}
