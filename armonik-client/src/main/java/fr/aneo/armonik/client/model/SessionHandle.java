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

import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.definition.TaskDefinition;
import io.grpc.ManagedChannel;

import static java.util.Objects.requireNonNull;

/**
 * Handle representing a session within the ArmoniK distributed computing platform.
 * <p>
 * A session is a logical container for tasks and associated data within the ArmoniK cluster [[1]](https://armonik.readthedocs.io/en/latest/content/armonik/glossary.html).
 * This handle provides the primary interface for task submission and session lifecycle management.
 * <p>
 * SessionHandle instances are responsible for:
 * <ul>
 *   <li>Submitting tasks within the session context</li>
 *   <li>Managing task output completion events and callbacks</li>
 *   <li>Coordinating session-scoped operations</li>
 * </ul>
 * <p>
 * This class is thread-safe and supports concurrent operations.
 *
 * @see TaskHandle
 * @see TaskDefinition
 * @see SessionInfo
 */
public final class SessionHandle {

  private final SessionInfo sessionInfo;
  private final TaskSubmitter taskSubmitter;

  /**
   * Constructs a new session handle with the specified session information and configuration.
   * <p>
   * This constructor initializes the handle with the necessary components for task submission
   * and output processing within the session context. The handle will use the provided
   * gRPC channel for all cluster communications.
   *
   * @param sessionInfo the immutable session metadata including session ID and configuration
   * @param sessionDefinition the session definition used for task default configurations
   * @param channel the gRPC channel for communicating with the ArmoniK cluster
   * @throws NullPointerException if any parameter is null
   * @see SessionInfo
   * @see SessionDefinition
   */
  SessionHandle(SessionInfo sessionInfo, SessionDefinition sessionDefinition, ManagedChannel channel) {
    requireNonNull(sessionInfo, "sessionInfo must not be null");
    requireNonNull(sessionDefinition, "sessionDefinition must not be null");
    requireNonNull(channel, "channel must not be null");

    this.sessionInfo = sessionInfo;
    this.taskSubmitter = new TaskSubmitter(sessionInfo.id(), sessionDefinition, channel);
  }

  /**
   * Returns the immutable session information associated with this handle.
   * <p>
   * The session information includes the unique session identifier, partition IDs,
   * and default task configuration that applies to tasks submitted within this session.
   *
   * @return the session metadata
   * @see SessionInfo
   */
  public SessionInfo sessionInfo() {
    return sessionInfo;
  }

  /**
   * Submits a new task for execution within this session.
   * <p>
   * This method creates and submits a task based on the provided task definition.
   * The task will inherit default configurations from the session unless explicitly
   * overridden in the task definition. Input blobs specified in the task definition
   * will be uploaded to the cluster, and output blob handles will be created for
   * expected outputs.
   * <p>
   * The returned {@link TaskHandle} provides access to task metadata and associated
   * input/output blobs. Task submission is asynchronous - the handle is returned
   * immediately while the actual submission continues in the background.
   *
   * @param taskDefinition the definition specifying task inputs, expected outputs,
   *                      payload, and optional task-specific configuration
   * @return a handle representing the submitted task
   * @throws NullPointerException if taskDefinition is null
   * @throws RuntimeException if task submission fails due to cluster communication issues
   * @see TaskHandle
   * @see TaskDefinition
   */
  public TaskHandle submitTask(TaskDefinition taskDefinition) {
    return taskSubmitter.submit(taskDefinition);
  }

  /**
   * Blocks until all currently pending task outputs are completed and the configured
   * task output listener has been invoked for each of them.
   * <p>
   * This method acts as a synchronization barrier for output processing. It takes a
   * snapshot of the outputs being watched at the moment of invocation and blocks the
   * calling thread until:
   * <ul>
   *   <li>each output has reached a terminal state (completed or failed), and</li>
   *   <li>the task output listener has been applied for each terminal output.</li>
   * </ul>
   * Outputs submitted or started after this method is called are not included in this wait.
   * If there are no pending outputs at call time, the method returns immediately.
   * <p>
   * This method does not cancel any ongoing operations and is safe to call concurrently.
   * Each concurrent call waits for the set of outputs that were in-flight at the time
   * of that specific invocation.
   *
   * @see BlobCompletionListener
   */
  public void awaitOutputsProcessed() {
    taskSubmitter.waitUntilFinished().toCompletableFuture().join();
  }

  @Override
  public String toString() {
    return "SessionHandle{" +
      "sessionInfo=" + sessionInfo +
      '}';
  }
}
