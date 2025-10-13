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

import fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataRequest.ResultIdentifier;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataRequest;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;


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

  final class AgentNotifier implements BlobListener {
    private final AgentFutureStub agentStub;
    private final String sessionId;
    private final String communicationToken;

    /**
     * Implementation of {@link BlobListener} that notifies the ArmoniK Agent via gRPC.
     * <p>
     * This notifier sends a {@code NotifyResultData} request to the Agent, informing it that
     * an output result is available in the shared data folder. The Agent can then:
     * </p>
     * <ul>
     *   <li>Mark the result as completed in the Control Plane</li>
     *   <li>Schedule dependent tasks that were waiting for this output</li>
     *   <li>Update task status to reflect output production</li>
     * </ul>
     *
     * <h2>Communication Protocol</h2>
     * <p>
     * The notification includes:
     * </p>
     * <ul>
     *   <li><strong>Communication token</strong>: Identifies the current task execution</li>
     *   <li><strong>Session ID</strong>: Identifies the session containing the result</li>
     *   <li><strong>Result ID</strong>: The blob ID of the output that is ready</li>
     * </ul>
     *
     * <h2>Thread Safety</h2>
     * <p>
     * This class is thread-safe. The gRPC stub handles concurrent calls appropriately.
     * However, since the Worker processes one task at a time, concurrent notifications
     * are not expected during normal operation.
     * </p>
     *
     * <h2>Error Handling</h2>
     * <p>
     * The current implementation does not explicitly handle gRPC errors. If the Agent
     * is unavailable or the notification fails, the error propagates to the caller
     * (typically {@link TaskOutput#write(byte[])} and similar methods).
     * </p>
     */
    AgentNotifier(AgentFutureStub agentStub, String sessionId, String communicationToken) {
      this.agentStub = agentStub;
      this.sessionId = sessionId;
      this.communicationToken = communicationToken;
    }

    /**
     * Notifies the ArmoniK Agent that a blob is ready via gRPC.
     * <p>
     * This method sends a {@code NotifyResultData} request containing the blob ID,
     * session ID, and communication token. The Agent receives this notification and
     * updates its internal state accordingly.
     * </p>
     * <p>
     * The gRPC call is asynchronous (uses {@code FutureStub}), so this method returns
     * quickly without waiting for the Agent's response.
     * </p>
     *
     * @param blobId the identifier of the blob that is ready; must not be {@code null}
     * @throws io.grpc.StatusRuntimeException if the gRPC call fails (network error,
     *                                        Agent unavailable, etc.)
     */
    @Override
    public void onBlobReady(BlobId blobId) {
      agentStub.notifyResultData(NotifyResultDataRequest.newBuilder()
                                                        .setCommunicationToken(communicationToken)
                                                        .addIds(ResultIdentifier.newBuilder()
                                                                                .setSessionId(sessionId)
                                                                                .setResultId(blobId.asString())
                                                                                .build())
                                                        .build());
    }
  }
}
