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
package fr.aneo.armonik.worker.domain.definition.blob;

import static java.util.Objects.requireNonNull;

/**
 * Base definition for blobs in ArmoniK's object storage.
 * <p>
 * A {@code BlobDefinition} represents metadata for a blob that can be:
 * </p>
 * <ul>
 *   <li>An input blob (data dependency) - includes both name and data</li>
 *   <li>An output blob (expected result) - includes only name</li>
 * </ul>
 * <p>
 * This sealed interface permits two categories:
 * </p>
 * <ul>
 *   <li>{@link InputBlobDefinition} - for blobs with data to be uploaded</li>
 *   <li>{@link OutputBlobDefinition} - for blobs that will be produced by tasks</li>
 * </ul>
 *
 * @see InputBlobDefinition
 * @see OutputBlobDefinition
 */
public sealed interface BlobDefinition permits InputBlobDefinition, OutputBlobDefinition {

  /**
   * Returns the name of this blob.
   * <p>
   * This name is used as metadata in ArmoniK's object storage.
   * The server generates a unique ID regardless of the name.
   * An empty string indicates no explicit name was provided.
   * </p>
   *
   * @return the blob name; never {@code null}, may be empty
   */
  String name();
}
