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
package fr.aneo.armonik.worker;


/**
 * Callback interface for receiving notifications when blob data becomes available.
 * <p>
 * A {@code BlobListener} is notified when output data has been written to the shared data folder
 * and is ready for consumption by the ArmoniK Agent or dependent tasks. This notification
 * mechanism is critical for ArmoniK's task dependency management and enables the Agent to
 * schedule dependent tasks as soon as their input data becomes available.
 * </p>
 *
 * <h2>Purpose</h2>
 * <p>
 * The listener pattern enables:
 * </p>
 * <ul>
 *   <li><strong>Asynchronous notification</strong>: Tasks signal output completion without blocking</li>
 *   <li><strong>Dependency resolution</strong>: Agent schedules dependent tasks when their inputs are ready</li>
 *   <li><strong>Dynamic graphs</strong>: Subtasks can be scheduled as soon as outputs are produced</li>
 *   <li><strong>Decoupling</strong>: {@link TaskOutput} doesn't need direct Agent knowledge</li>
 * </ul>
 *
 * @see TaskOutput
 * @see BlobId
 * @see AgentNotifier
 */
@FunctionalInterface
interface BlobListener {

  /**
   * Called when blob data has been written and is ready for consumption.
   * <p>
   * This method is invoked by {@link TaskOutput} after successfully writing output data
   * to the file system. Implementations should notify the appropriate consumers (typically
   * the ArmoniK Agent) that the blob is available.
   * </p>
   * <p>
   * <strong>Note:</strong> This method should complete quickly and avoid blocking operations.
   * </p>
   *
   * @param blobId the identifier of the blob that is now ready; never {@code null}
   */
  void onBlobReady(BlobId blobId);

}
