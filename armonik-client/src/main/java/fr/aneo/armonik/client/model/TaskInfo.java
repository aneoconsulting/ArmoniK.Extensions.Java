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
package fr.aneo.armonik.client.model;

/**
 * Immutable metadata describing a submitted task in the ArmoniK cluster.
 * <p>
 * This record encapsulates the essential information about a task after it has been
 * acknowledged and assigned identifiers by the ArmoniK cluster. The task information
 * becomes available through {@link TaskHandle#deferredTaskInfo()} once the cluster
 * processes the task submission.
 *
 * @param id the unique identifier assigned to the task by the ArmoniK cluster
 * @see TaskHandle#deferredTaskInfo()
 * @see TaskId
 */
record TaskInfo(TaskId id) {
}
