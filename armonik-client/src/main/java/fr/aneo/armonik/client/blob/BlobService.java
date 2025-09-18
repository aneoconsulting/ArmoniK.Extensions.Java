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
   * Allocates and associates blob handles required for a task submission.
   * <p>
   * This operation pre-allocates blob identifiers for:
   * <ul>
   *   <li>One payload handle to hold the serialized task manifest</li>
   *   <li>All declared output blob handles (mapped by output name)</li>
   *   <li>Inline input blob handles for inputs defined with {@link BlobDefinition} (mapped by input name)</li>
   * </ul>
   *
   * <p>
   * <strong>Important:</strong> Existing input handles that are already {@link BlobHandle} instances
   * in the task definition are preserved and not reallocated. Only inputs defined as
   * {@link BlobDefinition} (inline data) receive new blob handles for subsequent upload.
   *
   * <p>
   * The returned {@link BlobHandlesAllocation} provides organized access to all allocated handles:
   * <ul>
   *   <li>{@link BlobHandlesAllocation#payloadHandle()} - for constructing and uploading the task payload manifest</li>
   *   <li>{@link BlobHandlesAllocation#inputHandlesByName()} - for uploading inline input data via {@link #uploadBlobData}</li>
   *   <li>{@link BlobHandlesAllocation#outputHandlesByName()} - for task result retrieval after execution</li>
   * </ul>
   *
   * <p>
   * This method is typically called as part of the task submission workflow before uploading
   * input data and submitting the task to the ArmoniK cluster.
   *
   * @param sessionHandle  the ArmoniK session for which blob handles are allocated; must not be {@code null}
   * @param taskDefinition the task definition describing inputs and outputs; must not be {@code null}
   * @return a {@link BlobHandlesAllocation} bundling all allocated blob handles, never {@code null}
   * @throws NullPointerException if {@code session} or {@code taskDefinition} is {@code null}
   * @see #uploadBlobData(BlobHandle, BlobDefinition)
   * @see TaskDefinition#withInput(String, BlobHandle)
   * @see TaskDefinition#withInput(String, BlobDefinition)
   */

  BlobHandlesAllocation allocateBlobHandles(SessionHandle sessionHandle, TaskDefinition taskDefinition);

  /**
   * Creates input blobs in the provided session from the given definitions.
   * <p>
   * This operation both creates blob metadata and uploads the content in a single step.
   * Each {@link BlobDefinition} provides the raw bytes for one blob. The position in the
   * input list corresponds to the same position in the returned handle list.
   *
   * <p>
   * <strong>Performance Note:</strong> For task submission workflows or when fine-grained control over
   * blob allocation and upload timing is needed, consider using {@link #allocateBlobHandles(SessionHandle, TaskDefinition)}
   * followed by {@link #uploadBlobData(BlobHandle, BlobDefinition)} for better resource management
   * and integration with the task submission process.
   * </p>
   *
   * @param sessionHandle   the ArmoniK session that will own the created blobs; must not be {@code null}
   * @param blobDefinitions the list of blob definitions containing the blob data; must not be {@code null},
   *                        and no element should be {@code null}
   * @return an ordered list of handles for the created blobs, with the same size as {@code blobDefinitions}
   * @throws NullPointerException if {@code session}, {@code blobDefinitions}, or any element in
   *                              {@code blobDefinitions} is {@code null}
   */
  List<BlobHandle> createBlobs(SessionHandle sessionHandle, List<BlobDefinition> blobDefinitions);

  /**
   * Uploads binary content for a pre-allocated blob handle.
   * <p>
   * This operation uploads the blob data asynchronously and is designed to handle
   * large payloads efficiently by streaming the data in chunks to the ArmoniK cluster.
   * The blob handle must have been allocated previously using {@link #allocateBlobHandles(SessionHandle, TaskDefinition)}.
   *
   * <p>
   * This method is typically used as part of the task submission workflow to upload inline input data
   * to blob handles that were pre-allocated through {@link #allocateBlobHandles(SessionHandle, TaskDefinition)}.
   * The operation completes when all data has been successfully transferred and stored.
   *
   * @param blobHandle     the pre-created blob handle to upload data for; must not be {@code null}
   *                       and must be in a state that accepts data upload
   * @param blobDefinition the blob definition containing the binary data to upload; must not be {@code null}
   * @return a {@link CompletionStage} that completes successfully when the upload is finished,
   * or completes exceptionally if the upload fails
   * @throws NullPointerException if {@code handle} or {@code blobDefinition} is {@code null}
   * @throws IllegalStateException if the blob handle is not in a valid state for data upload
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
