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
package fr.aneo.armonik.worker.internal;

import com.google.protobuf.Duration;
import fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.SubmitTasksRequest.TaskCreation;
import fr.aneo.armonik.worker.domain.*;
import fr.aneo.armonik.worker.domain.internal.Payload;
import fr.aneo.armonik.worker.domain.internal.TaskService;
import fr.aneo.armonik.worker.internal.concurrent.Futures;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import static fr.aneo.armonik.api.grpc.v1.Objects.TaskOptions;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.SubmitTasksRequest;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static java.util.Objects.requireNonNull;

/**
 * Service responsible for submitting tasks to ArmoniK via the Agent gRPC API.
 * <p>
 * This service handles task creation and submission within a session. Tasks submitted
 * through this service are subtasks of the currently executing task and will be processed
 * according to their configuration and dependencies.
 * </p>
 *
 * <h2>Task Structure</h2>
 * <p>
 * Each task submitted consists of:
 * </p>
 * <ul>
 *   <li><strong>Payload</strong>: A special input blob containing the mapping between logical
 *       names and blob IDs for inputs and outputs</li>
 *   <li><strong>Data Dependencies</strong>: Input blobs that must be available before the task
 *       can execute</li>
 *   <li><strong>Expected Outputs</strong>: Output blobs that the task is expected to produce</li>
 *   <li><strong>Task Configuration</strong>: Execution parameters (priority, retries, partition, etc.)</li>
 * </ul>
 *
 * <h2>Asynchronous Submission</h2>
 * <p>
 * Task submission is asynchronous. The {@link #submitTask} method returns a {@link CompletionStage}
 * that completes when the Agent acknowledges the task submission. The actual task execution may
 * occur later, once all data dependencies are satisfied.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe. Multiple tasks can be submitted concurrently from different threads.
 * </p>
 *
 * @see TaskConfiguration
 * @see GrpcBlobService
 * @see Payload
 * @see TaskContext
 */
class GrpcTaskService implements TaskService {

  private final AgentFutureStub agentStub;
  private final SessionId sessionId;
  private final String communicationToken;

  /**
   * Creates a new task service for submitting tasks within a session.
   *
   * @param agentStub          the gRPC stub for communicating with the Agent; must not be {@code null}
   * @param sessionId          the session identifier; must not be {@code null}
   * @param communicationToken the communication token for this execution context; must not be {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   */
  GrpcTaskService(AgentFutureStub agentStub, SessionId sessionId, String communicationToken) {
    this.agentStub = agentStub;
    this.sessionId = sessionId;
    this.communicationToken = communicationToken;
  }

  /**
   * Submits a task for execution to ArmoniK.
   * <p>
   * This method creates a task with the specified dependencies, outputs, payload, and configuration,
   * then submits it to ArmoniK for scheduling and execution. The task will not execute until all
   * its data dependencies are available.
   *
   * <h3>Validation Rules</h3>
   * <p>
   * All parameters must satisfy:
   * </p>
   * <ul>
   *   <li>{@code inputIds} must not be {@code null} or empty</li>
   *   <li>{@code outputIds} must not be {@code null} or empty</li>
   *   <li>{@code payloadId} must not be {@code null}</li>
   * </ul>
   *
   * @param inputIds          collection of blob IDs for task inputs (data dependencies);
   *                          must not be {@code null} or empty
   * @param outputIds         collection of blob IDs for expected task outputs;
   *                          must not be {@code null} or empty
   * @param payloadId         blob ID of the payload containing the input/output mapping;
   *                          must not be {@code null}
   * @param taskConfiguration configuration for this task;
   *                          must not be {@code null}
   * @return a completion stage that completes with task information when the Agent acknowledges
   * the submission; never {@code null}
   * @throws NullPointerException     if any parameter is {@code null}
   * @throws IllegalArgumentException if {@code inputIds} or {@code outputIds} is empty
   */
  @Override
  public CompletionStage<TaskInfo> submitTask(Collection<BlobId> inputIds, Collection<BlobId> outputIds, BlobId payloadId, TaskConfiguration taskConfiguration) {
    requireNonNull(inputIds, "inputIds must not be null");
    requireNonNull(outputIds, "outputIds must not be null");
    requireNonNull(payloadId, "payloadId must not be null");

    if (inputIds.isEmpty() || outputIds.isEmpty())
      throw new IllegalArgumentException("inputIds and outputIds must not be empty");

    var responseStage = Futures.toCompletionStage(
      agentStub.submitTasks(SubmitTasksRequest.newBuilder()
                                              .setSessionId(sessionId.asString())
                                              .setCommunicationToken(communicationToken)
                                              .addTaskCreations(toTaskCreation(inputIds, outputIds, payloadId, taskConfiguration))
                                              .build())
    );

    return responseStage.thenApply(response -> new TaskInfo(TaskId.from(response.getTaskInfos(0).getTaskId())));
  }

  /**
   * Converts task parameters to a gRPC task creation request.
   * <p>
   * This method builds a {@link TaskCreation} message containing:
   * </p>
   * <ul>
   *   <li>Payload ID (reference to the payload blob)</li>
   *   <li>Data dependencies (input blob IDs as strings)</li>
   *   <li>Expected output keys (output blob IDs as strings)</li>
   *   <li>Task options (from the provided task configuration)</li>
   * </ul>
   *
   * @param inputBlobIds      collection of input blob IDs; must not be {@code null}
   * @param outputBlobIds     collection of output blob IDs; must not be {@code null}
   * @param payloadId         payload blob ID; must not be {@code null}
   * @param taskConfiguration task configuration; }
   * @return a task creation message ready for submission; never {@code null}
   */
  private static TaskCreation toTaskCreation(Collection<BlobId> inputBlobIds,
                                             Collection<BlobId> outputBlobIds,
                                             BlobId payloadId,
                                             TaskConfiguration taskConfiguration) {
    var builder = TaskCreation.newBuilder()
                              .setPayloadId(payloadId.asString())
                              .addAllDataDependencies(inputBlobIds.stream().map(BlobId::asString).toList())
                              .addAllExpectedOutputKeys(outputBlobIds.stream().map(BlobId::asString).toList());
    if (taskConfiguration != null) {
      builder.setTaskOptions(toTaskOptions(taskConfiguration));
    }

    return builder.build();
  }

  /**
   * Converts a domain {@link TaskConfiguration} to a gRPC {@link TaskOptions} message.
   * <p>
   * This conversion transforms the domain model into the wire format used by the Agent API.
   * All fields are mapped directly:
   * </p>
   * <ul>
   *   <li><strong>maxDuration</strong>: Converted to protobuf Duration (seconds + nanos)</li>
   *   <li><strong>priority</strong>: Task priority level for scheduling</li>
   *   <li><strong>maxRetries</strong>: Maximum number of retry attempts on failure</li>
   *   <li><strong>partitionId</strong>: Target partition for task execution</li>
   *   <li><strong>options</strong>: Custom key-value pairs for application-specific settings</li>
   * </ul>
   *
   * @param taskConfiguration the task configuration to convert; must not be {@code null}
   * @return the equivalent gRPC task options message; never {@code null}
   * @throws NullPointerException if {@code taskConfiguration} is {@code null}
   */
  private static TaskOptions toTaskOptions(TaskConfiguration taskConfiguration) {
    return TaskOptions.newBuilder()
                      .setMaxDuration(Duration.newBuilder()
                                              .setSeconds(taskConfiguration.maxDuration().getSeconds())
                                              .setNanos(taskConfiguration.maxDuration().getNano()))
                      .setPriority(taskConfiguration.priority())
                      .setMaxRetries(taskConfiguration.maxRetries())
                      .setPartitionId(taskConfiguration.partitionId())
                      .putAllOptions(taskConfiguration.options())
                      .build();
  }
}
