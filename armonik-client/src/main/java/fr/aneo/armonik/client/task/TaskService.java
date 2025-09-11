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
 *
 * @see TaskHandle
 * @see TaskDefinition
 * @see TaskMetadata
 * @see SessionHandle
 */

public interface TaskService {

  /**
   * Submits a task within the given session.
   *
   * @param sessionHandle            the session in which to submit the task; must not be {@code null}
   * @param inputs             map of input blob handles keyed by logical input names; must not be {@code null}
   * @param outputs            map of expected output blob handles keyed by logical output names; must not be {@code null}
   * @param taskConfiguration  configuration to apply to the task; may be {@code null} to use defaults
   * @return a handle to the submitted task
   * @throws NullPointerException if {@code session}, {@code inputs}, or {@code outputs} is {@code null}
   */
  TaskHandle submitTask(SessionHandle sessionHandle,
                        Map<String, BlobHandle> inputs,
                        Map<String, BlobHandle> outputs,
                        TaskConfiguration taskConfiguration);
}
