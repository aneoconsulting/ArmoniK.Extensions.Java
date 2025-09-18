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
package fr.aneo.armonik.client.payload;

import fr.aneo.armonik.client.blob.BlobDefinition;

import java.util.Map;
import java.util.UUID;

/**
 * Strategy for serializing task input/output blob identifier mappings into payload manifests.
 * <p>
 * A payload manifest is a compact representation of all blob identifiers associated with a task,
 * including both inputs and expected outputs. The ArmoniK runtime uses this manifest to resolve
 * blob references during task execution.
 *
 * <p>
 * Implementations of this interface define the format and encoding used for the payload manifest.
 * The default implementation {@link JsonPayloadSerializer} uses JSON encoding, but alternative
 * formats (binary, protobuf, etc.) can be implemented as needed.
 *
 * <p>
 * This interface is typically used internally by the ArmoniK client during task submission
 * to create the payload blob that accompanies each submitted task.
 *
 * @see JsonPayloadSerializer
 * @see fr.aneo.armonik.client.blob.BlobDefinition
 * @see fr.aneo.armonik.client.task.TaskDefinition
 */

public interface PayloadSerializer {

  /**
   * Serializes the mapping of input and output identifiers into a payload manifest.
   * <p>
   * This method takes the resolved blob UUIDs for all task inputs and outputs and encodes
   * them into a serialized format. The resulting {@link BlobDefinition} contains the binary
   * representation of the manifest that will be uploaded as the task's payload blob.
   *
   * <p>
   * The payload manifest serves as a reference map that the ArmoniK runtime uses to locate
   * and bind input/output blobs to the executing task. Both input and output mappings are
   * included even though outputs don't exist yet, as they represent the expected results
   * the task should produce.
   *
   * @param inputIds   mapping from logical input names to their resolved blob identifiers;
   *                   must not be {@code null}, but may be empty
   * @param outputIds  mapping from logical output names to their resolved blob identifiers;
   *                   must not be {@code null}, but may be empty
   * @return a {@link BlobDefinition} containing the serialized payload manifest bytes; never {@code null}
   * @throws NullPointerException if {@code inputIds} or {@code outputIds} is {@code null}
   * @throws RuntimeException if serialization fails due to encoding errors
   */

  BlobDefinition serialize(Map<String, UUID> inputIds, Map<String, UUID> outputIds);
}
