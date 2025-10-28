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
package fr.aneo.armonik.client.definition.blob;

import fr.aneo.armonik.client.model.BlobHandle;

/**
 * Interface representing a blob definition in the ArmoniK cluster.
 * <p>
 * A blob represents data content that can be associated with tasks as inputs or outputs.
 * This base class provides common properties for all types of blob definitions:
 * <ul>
 *   <li><b>Name</b>: A user-defined identifier for the blob</li>
 *   <li><b>Manual Deletion</b>: A flag indicating whether the user is responsible for
 *       explicitly deleting the blob data from the underlying object storage</li>
 * </ul>
 *
 * <h2>Manual Deletion Policy</h2>
 * When {@code manualDeletion} is set to {@code true}, the user must explicitly delete the blob data from the
 * underlying object storage. When set to {@code false} (default), ArmoniK automatically manages the blob lifecycle and
 * cleans up data when no longer needed.
 *
 * @see InputBlobDefinition
 * @see OutputBlobDefinition
 * @see BlobHandle
 */
public sealed interface BlobDefinition permits InputBlobDefinition, OutputBlobDefinition {

  /**
   * Returns the user-defined name of this blob.
   * <p>
   * The name serves as a logical identifier and is never {@code null}.
   * An empty string indicates an unnamed blob.
   *
   * @return the blob name, never null
   */
  String name();

  /**
   * Returns whether manual deletion is required for this blob.
   * <p>
   * When {@code true}, the user must explicitly delete the blob data from the
   * underlying object storage. When {@code false}, ArmoniK automatically manages
   * the blob lifecycle.
   *
   * @return true if manual deletion is required, false for automatic cleanup
   */
  boolean manualDeletion();
}
