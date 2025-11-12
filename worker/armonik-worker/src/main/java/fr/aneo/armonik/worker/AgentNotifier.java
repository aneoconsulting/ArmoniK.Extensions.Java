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

import fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataRequest;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.*;
import static io.grpc.Status.Code.DEADLINE_EXCEEDED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link BlobListener} that notifies the ArmoniK Agent via gRPC.
 * <p>
 * This notifier sends a {@code NotifyResultData} request to the Agent, informing it that
 * an output result is available in the shared data folder. The Agent can then:
 * </p>
 * <ul>
 *   <li>Mark the result as completed in the Control Plane</li>
 *   <li>Schedule dependent tasks that were waiting for this output</li>
 *   <li>Update the task status to reflect output production</li>
 * </ul>
 *
 * <h2>Synchronous Notification</h2>
 * <p>
 * <strong>Important:</strong> This implementation blocks until the Agent acknowledges
 * the notification. This synchronous behavior is critical to prevent race conditions
 * between output notification and task completion reporting.
 * </p>
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
 * If the Agent is unavailable or the notification fails, an {@link ArmoniKException}
 * is thrown with details about the failure. This prevents the Worker from reporting
 * task success when outputs were not properly registered.
 * </p>
 *
 * @see BlobListener
 * @see TaskOutput
 */
public final class AgentNotifier implements BlobListener {
  private static final Logger logger = LoggerFactory.getLogger(fr.aneo.armonik.worker.AgentNotifier.class);
  private static final int DEFAULT_NOTIFICATION_TIMEOUT = 30_000;

  private final AgentFutureStub agentStub;
  private final SessionId sessionId;
  private final String communicationToken;
  private final int timeout;


  AgentNotifier(AgentFutureStub agentStub, SessionId sessionId, String communicationToken) {
    this(agentStub, sessionId, communicationToken, DEFAULT_NOTIFICATION_TIMEOUT);
  }

  AgentNotifier(AgentFutureStub agentStub, SessionId sessionId, String communicationToken, int timeoutInMillisecond) {
    this.agentStub = agentStub;
    this.sessionId = sessionId;
    this.communicationToken = communicationToken;
    this.timeout = timeoutInMillisecond;
  }

  /**
   * Notifies the ArmoniK Agent that a blob is ready via gRPC.
   * <p>
   * This method sends a {@code NotifyResultData} request containing the blob ID,
   * session ID, and communication token. <strong>The method blocks</strong> until
   * the Agent acknowledges the notification or the timeout expires.
   * </p>
   * <p>
   * This synchronous behavior is intentional and necessary to prevent race conditions
   * where the Worker reports task completion before the Agent has registered the output.
   * </p>
   *
   * @param blobId the identifier of the blob that is ready; must not be {@code null}
   * @throws ArmoniKException if the notification times out, the gRPC call fails,
   *                          or the thread is interrupted
   */
  public void onBlobReady(BlobId blobId) {
    logger.info("Notifying Agent that blob is ready: blobId={}, sessionId={}", blobId.asString(), sessionId);
    try {
      var request = buildRequest(blobId);
      var response = agentStub.withDeadlineAfter(timeout, MILLISECONDS).notifyResultData(request).get();
      logger.info("Agent acknowledged blob notification: blobId={}, response={}", blobId.asString(), response);

    } catch (ExecutionException e) {
      handleExecutionFailure(blobId, e);

    } catch (InterruptedException e) {
      handleInterruption(blobId, e);
    }
  }

  private void handleExecutionFailure(BlobId blobId, ExecutionException e) {
    if (e.getCause() instanceof StatusRuntimeException sre && sre.getStatus().getCode() == DEADLINE_EXCEEDED) {
      logger.error("Agent deadline exceeded: blobId={}, timeout={}ms", blobId.asString(), timeout);
      throw new ArmoniKException("Agent did not acknowledge blob " + blobId.asString() + " within " + (timeout / 1000) + " seconds", sre);
    }

    logger.error("Agent notification failed: blobId={}", blobId.asString(), e.getCause());
    throw new ArmoniKException("Failed to notify Agent about blob " + blobId.asString(), e.getCause());
  }

  private NotifyResultDataRequest buildRequest(BlobId blobId) {
    return NotifyResultDataRequest.newBuilder()
                                  .setCommunicationToken(communicationToken)
                                  .addIds(NotifyResultDataRequest.ResultIdentifier.newBuilder()
                                                                                  .setSessionId(sessionId.asString())
                                                                                  .setResultId(blobId.asString())
                                                                                  .build())
                                  .build();
  }

  private static void handleInterruption(BlobId blobId, InterruptedException e) {
    Thread.currentThread().interrupt();
    logger.error("Interrupted while notifying Agent: blobId={}", blobId.asString());
    throw new ArmoniKException("Interrupted while notifying Agent about blob " + blobId.asString(), e);
  }
}
