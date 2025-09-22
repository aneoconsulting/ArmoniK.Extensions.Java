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
package fr.aneo.armonik.client.task;

import fr.aneo.armonik.client.blob.BlobHandle;
import fr.aneo.armonik.client.session.SessionHandle;

import java.util.Map;

/**
 * Service API for submitting tasks to ArmoniK.
 * <p>
 * This service provides the core functionality for task submission within an ArmoniK session.
 * Tasks are submitted with input data, expected outputs, and an optional payload containing
 * task-specific configuration or data.
 *
 * @see TaskHandle
 * @see TaskDefinition
 * @see TaskMetadata
 * @see TaskConfiguration
 * @see SessionHandle
 * @see BlobHandle
 * @see fr.aneo.armonik.client.blob.BlobHandlesAllocation
 */

public interface TaskService {

  /**
   * Submits a task within the given session.
   * <p>
   * The task will be executed asynchronously within the ArmoniK infrastructure. Payload, input and output
   * blob handles must be pre-allocated and properly configured before submission.
   *
   * @param sessionHandle            the session in which to submit the task; must not be {@code null}
   * @param inputs             map of input blob handles keyed by logical input names; must not be {@code null}
   * @param outputs            map of expected output blob handles keyed by logical output names; must not be {@code null}
   * @param payload            the payload blob handle containing task-specific data or configuration; may be {@code null}
   * @param taskConfiguration  configuration to apply to the task; may be {@code null} to use defaults
   * @return a handle to the submitted task; never {@code null}
   * @throws NullPointerException if {@code session}, {@code inputs}, or {@code outputs}, or {@code payload} is {@code null}
   * @throws IllegalArgumentException if any of the blob handles are invalid or if the session is not active
   */
  TaskHandle submitTask(SessionHandle sessionHandle,
                        Map<String, BlobHandle> inputs,
                        Map<String, BlobHandle> outputs,
                        BlobHandle payload,
                        TaskConfiguration taskConfiguration);
}
