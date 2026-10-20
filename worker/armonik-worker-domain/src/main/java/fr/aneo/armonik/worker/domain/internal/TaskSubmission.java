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

import fr.aneo.armonik.worker.domain.BlobId;
import fr.aneo.armonik.worker.domain.TaskConfiguration;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * The data needed to submit one task to the Agent.
 *
 * @param inputIds          blob IDs of the task inputs (data dependencies); must not be {@code null}
 * @param outputIds         blob IDs of the expected task outputs; must not be {@code null}
 * @param payloadId         blob ID of the payload containing the input/output mapping; must not be {@code null}
 * @param taskConfiguration configuration of the task; may be {@code null} to use the session defaults
 * @see TaskService#submitTasks(java.util.List)
 */
public record TaskSubmission(Collection<BlobId> inputIds,
                             Collection<BlobId> outputIds,
                             BlobId payloadId,
                             TaskConfiguration taskConfiguration) {

  public TaskSubmission {
    requireNonNull(inputIds, "inputIds must not be null");
    requireNonNull(outputIds, "outputIds must not be null");
    requireNonNull(payloadId, "payloadId must not be null");
  }
}
