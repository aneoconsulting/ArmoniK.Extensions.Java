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

import java.util.Map;

/**
 * Groups together the blob handles allocated for a single task submission.
 * <p>
 * A {@code BlobHandlesAllocation} bundles all blob identifiers required during the submission process:
 * <ul>
 *   <li>the payload handle, which will hold the serialized manifest of inputs and outputs,</li>
 *   <li>the output handles, mapped by output name, which represent the results to be produced by the task,</li>
 *   <li>the input handles, mapped by input name, which are allocated for inline input values that must be uploaded.</li>
 * </ul>
 *
 * <p>
 * This record is typically created by {@link BlobService#allocateBlobHandles} and consumed by
 * {@code ArmoniKClient} during task submission to orchestrate input uploads, payload construction, and task creation.
 *  @param payloadHandle      the handle reserved for the payload manifest blob
 *  @param inputHandlesByName  mapping from inline input names to their allocated blob handles
 *  @param outputHandlesByName mapping from output names to their allocated blob handles
 */

public record BlobHandlesAllocation(
  BlobHandle payloadHandle,
  Map<String, BlobHandle> inputHandlesByName,
  Map<String, BlobHandle> outputHandlesByName
) {
}
