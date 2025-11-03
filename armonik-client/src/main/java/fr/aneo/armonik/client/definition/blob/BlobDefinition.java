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

import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.model.BlobHandle;

/**
 * Metadata definition for a blob in the ArmoniK cluster.
 * <p>
 * A {@code BlobDefinition} specifies cluster-level metadata for blob storage,
 * including the monitoring name and deletion policy. This metadata is independent
 * of how the blob is referenced in task definitions.
 * <p>
 * <strong>Important Distinction:</strong>
 * <ul>
 *   <li><strong>Blob name ({@link #name()}):</strong> Metadata stored in the ArmoniK
 *       cluster for monitoring and management. This is cluster-wide and visible across
 *       all tasks that reference this blob.</li>
 *   <li><strong>Parameter name:</strong> The logical name used in
 *       {@link TaskDefinition#withInput(String, InputBlobDefinition)} or
 *       {@link TaskDefinition#withOutput(String, OutputBlobDefinition)} to reference
 *       the blob within a specific task's contract. This is task-specific.</li>
 * </ul>
 * <p>
 * <strong>Example - Shared Blob with Different Parameter Names:</strong>
 * <pre>{@code
 * // Create blob with monitoring name "training_dataset"
 * var dataset = InputBlobDefinition.from("training_dataset", dataBytes);
 *
 * // Same blob referenced with different parameter names in different tasks
 * task1.withInput("trainingData", datasetHandle);  // Parameter name: "trainingData"
 * task2.withInput("validationSet", datasetHandle); // Parameter name: "validationSet"
 * task3.withInput("input", datasetHandle);         // Parameter name: "input"
 *
 * // All three tasks reference the same blob, which appears in ArmoniK monitoring
 * // as "training_dataset" regardless of the parameter names used.
 * }</pre>
 *
 * @see InputBlobDefinition
 * @see OutputBlobDefinition
 * @see BlobHandle
 */
public sealed interface BlobDefinition permits InputBlobDefinition, OutputBlobDefinition {

  /**
   * Returns the cluster-level name for monitoring and management.
   * <p>
   * This name is stored as metadata in the ArmoniK cluster and used for:
   * <ul>
   *   <li>Monitoring and observability dashboards</li>
   *   <li>Debugging and diagnostics</li>
   *   <li>Blob lifecycle management</li>
   * </ul>
   * <p>
   * This is independent of parameter names used in task definitions.
   * An empty string is a valid value for blobs that don't require
   * specific monitoring identification.
   *
   * @return the blob name for cluster metadata, may be empty
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
