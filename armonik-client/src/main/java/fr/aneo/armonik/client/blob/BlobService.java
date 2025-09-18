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
import fr.aneo.armonik.client.task.TaskDefinition;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Service API for creating and referencing blobs (task inputs and outputs) in ArmoniK.
 * <p>
 * Blobs in ArmoniK represent the data inputs and outputs of tasks. This service provides
 * operations to manage blob lifecycle including metadata creation, content upload, and
 * data retrieval within the context of an ArmoniK session.
 *
 * <p>
 * All blob operations are scoped to a specific {@link SessionHandle}, ensuring proper
 * isolation and resource management within the ArmoniK cluster.
 *
 * @see BlobHandle
 * @see BlobMetadata
 * @see BlobDefinition
 * @see SessionHandle
 * @see TaskDefinition
 */
public interface BlobService {

  /**
   * Creates metadata entries for a given number of blobs in the provided session.
   * <p>
   * This operation pre-allocates blob identifiers and metadata without uploading any content.
   * It can be used for both input and output blobs when you need to separate metadata creation
   * from data upload.
   *
   * <p>
   * The blob metadata becomes available asynchronously through the returned handles.
   * For large input blobs, this approach combined with {@link #uploadBlobData(BlobHandle, BlobDefinition)}
   * provides better control over upload timing and resource management compared to
   * {@link #createBlobs(SessionHandle, List)}.
   * </p>
   *
   * @param sessionHandle the ArmoniK session that will own the blobs; must not be {@code null}
   * @param count   the number of blob metadata entries to create; must be {@code >= 0}
   * @return a list of blob handles with pre-allocated identifiers (empty list when {@code count == 0})
   * @throws NullPointerException     if {@code session} is {@code null}
   * @throws IllegalArgumentException if {@code count} is negative
   */
  List<BlobHandle> createBlobMetaData(SessionHandle sessionHandle, int count);

  /**
   * Creates input blobs in the provided session from the given definitions.
   * <p>
   * This operation both creates blob metadata and uploads the content synchronously.
   * Each {@link BlobDefinition} provides the raw bytes for one blob. The position in the
   * input list corresponds to the same position in the returned handle list.
   *
   * <p>
   * <strong>Performance Note:</strong> For large blobs or when fine-grained control over
   * upload timing is needed, consider using {@link #createBlobMetaData(SessionHandle, int)}
   * followed by {@link #uploadBlobData(BlobHandle, BlobDefinition)} for better resource management.
   * </p>
   *
   * @param sessionHandle         the ArmoniK session that will own the created blobs; must not be {@code null}
   * @param blobDefinitions the list of blob definitions containing the blob data; must not be {@code null},
   *                        and no element should be {@code null}
   * @return an ordered list of handles for the created blobs, with the same size as {@code blobDefinitions}
   * @throws NullPointerException if {@code session}, {@code blobDefinitions}, or any element in
   *                              {@code blobDefinitions} is {@code null}
   */
  List<BlobHandle> createBlobs(SessionHandle sessionHandle, List<BlobDefinition> blobDefinitions);


  /**
   * Uploads binary content for a pre-created blob handle.
   * <p>
   * This operation uploads the blob data asynchronously and is designed to handle
   * large payloads efficiently by streaming the data in chunks to the ArmoniK cluster.
   * The blob handle must have been created previously using {@link #createBlobMetaData(SessionHandle, int)}.
   *
   * <p>
   * The operation completes when all data has been successfully transferred and stored.
   *
   * @param blobHandle         the pre-created blob handle to upload data for; must not be {@code null}
   *                       and must be in a state that accepts data upload
   * @param blobDefinition the blob definition containing the binary data to upload; must not be {@code null}
   * @return a {@link CompletionStage} that completes successfully when the upload is finished,
   * or completes exceptionally if the upload fails
   * @throws NullPointerException if {@code handle} or {@code blobDefinition} is {@code null}
   */
  CompletionStage<Void> uploadBlobData(BlobHandle blobHandle, BlobDefinition blobDefinition);

  /**
   * Downloads the complete binary content of a blob by its identifier.
   * <p>
   * This operation retrieves the full blob content from the ArmoniK cluster.
   * The blob must exist and be accessible within the specified session scope.
   *
   * @param sessionHandle the ArmoniK session that owns the blob; must not be {@code null}
   * @param blobId  the unique identifier of the blob to download; must not be {@code null}
   * @return a {@link CompletionStage} that completes with the blob data as a byte array,
   * or completes exceptionally if the blob cannot be found or downloaded
   * @throws NullPointerException if {@code session} or {@code blobId} is {@code null}
   */
  CompletionStage<byte[]> downloadBlob(SessionHandle sessionHandle, UUID blobId);
}
