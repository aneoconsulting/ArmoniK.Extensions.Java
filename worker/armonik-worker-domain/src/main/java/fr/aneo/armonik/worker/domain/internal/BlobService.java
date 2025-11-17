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
package fr.aneo.armonik.worker.domain.internal;

import fr.aneo.armonik.worker.domain.BlobHandle;
import fr.aneo.armonik.worker.domain.definition.blob.BlobDefinition;
import fr.aneo.armonik.worker.domain.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.blob.OutputBlobDefinition;

import java.util.Map;

/**
 * Service responsible for creating blobs in ArmoniK's object storage.
 *
 * @see InputBlobDefinition
 * @see OutputBlobDefinition
 * @see BlobHandle
 */
public interface BlobService {
  /**
   * Creates multiple input blobs with their data from input blob definitions.
   *
   * @param inputBlobDefinitions map of logical names to input blob definitions
   * @return map of logical names to blob handles
   * @throws NullPointerException if inputBlobDefinitions is null
   * @see InputBlobDefinition
   * @see #prepareBlobs(Map)
   */
  Map<String, BlobHandle> createBlobs(Map<String, InputBlobDefinition> inputBlobDefinitions);

  /**
   * Creates a single input blob with its data from an input blob definition.
   *
   * @param inputBlobDefinition input blob definition containing the data to upload
   * @return blob handle representing the created blob
   * @throws NullPointerException if inputBlobDefinition is null
   * @see InputBlobDefinition
   * @see #createBlobs(Map)
   * @see #prepareBlobs(Map)
   */
  BlobHandle createBlob(InputBlobDefinition inputBlobDefinition);

  /**
   * Prepares multiple blobs by creating their metadata without uploading data.
   * <p>
   * This method creates blob metadata. The actual data will be provided later, either:
   * </p>
   * <ul>
   *   <li>By task execution (for output blobs)</li>
   *   <li>By writing to files in the shared folder (for input blobs)</li>
   * </ul>
   * <p>
   * Common use cases:
   * </p>
   * <ul>
   *   <li><strong>Output blobs</strong>: Expected outputs that tasks will produce</li>
   *   <li><strong>Input blobs</strong>: Large input data written to files after metadata creation</li>
   *   <li><strong>Delayed upload</strong>: Create metadata early, provide data later</li>
   * </ul>
   * <p>
   * The returned blob handles can be used as data dependencies for downstream tasks,
   * enabling task graph construction before all data is available.
   * </p>
   *
   * @param blobDefinitions map of logical names to blob definitions
   * @return map of logical names to blob handles
   * @throws NullPointerException if blobDefinitions is null
   * @see BlobDefinition
   * @see OutputBlobDefinition
   * @see InputBlobDefinition
   * @see #createBlobs(Map)
   */
  <T extends BlobDefinition> Map<String, BlobHandle> prepareBlobs(Map<String, T> blobDefinitions);
}
