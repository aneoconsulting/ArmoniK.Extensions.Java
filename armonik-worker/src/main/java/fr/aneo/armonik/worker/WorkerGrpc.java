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

import fr.aneo.armonik.api.grpc.v1.Objects.Output.Error;
import fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus;
import fr.aneo.armonik.api.grpc.v1.worker.WorkerGrpc.WorkerImplBase;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static fr.aneo.armonik.api.grpc.v1.Objects.Empty;
import static fr.aneo.armonik.api.grpc.v1.Objects.Output;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.*;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus.NOT_SERVING;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus.SERVING;

/**
 * gRPC service implementation for the ArmoniK Worker protocol.
 * <p>
 * {@code WorkerGrpc} implements the Worker service as defined in the ArmoniK API protobuf specification.
 * It acts as the gRPC layer that receives task processing requests from the ArmoniK Agent, delegates
 * execution to a {@link TaskProcessor}, and reports task outcomes back to the Agent.
 *
 * <h2>Service Responsibilities</h2>
 * <ul>
 *   <li><strong>Task Processing</strong>: Receives {@code ProcessRequest} from the Agent,
 *       creates a {@link TaskContext}, invokes the {@link TaskProcessor}, and returns the outcome</li>
 *   <li><strong>Health Checking</strong>: Reports the Worker's health status to allow the Agent
 *       to monitor Worker availability and readiness</li>
 *   <li><strong>Exception Handling</strong>: Catches and converts any exceptions thrown during
 *       task processing into proper error responses</li>
 *   <li><strong>Status Management</strong>: Tracks serving status to indicate when the Worker
 *       is busy processing a task</li>
 * </ul>
 *
 * <h2>Architecture</h2>
 * <p>
 * This class bridges the gRPC protocol layer and the application logic layer:
 * <pre>
 * Agent → WorkerGrpc → TaskContextFactory → TaskContext → TaskProcessor → Application Logic
 *                ↓
 *         Output/Error
 * </pre>
 *
 * <h2>Health Check Behavior</h2>
 * <p>
 * The Worker reports its serving status through the {@link #healthCheck(Empty, StreamObserver)} method:
 * <ul>
 *   <li>{@link ServingStatus#SERVING}: Worker is idle and ready to accept tasks</li>
 *   <li>{@link ServingStatus#NOT_SERVING}: Worker is currently processing a task</li>
 * </ul>
 * <p>
 * The Agent uses this status to determine whether to assign new tasks to the Worker.
 *
 * <h2>Exception Handling Strategy</h2>
 * <p>
 * Any exception thrown during task processing is caught and handled as follows:
 * <ol>
 *   <li>The exception message is extracted (or {@code toString()} if message is {@code null})</li>
 *   <li>A {@link TaskOutcome.Error} is created with the exception message</li>
 *   <li>The error is converted to a gRPC {@link Output} message</li>
 *   <li>The Worker's status is restored to {@link ServingStatus#SERVING}</li>
 *   <li>The response is sent back to the Agent</li>
 * </ol>
 * <p>
 * This ensures that exceptions never propagate as gRPC errors, allowing the Agent to properly
 * track task failures and potentially retry them.
 *
 * <h2>Usage</h2>
 * <p>
 * This class is instantiated and registered by {@link ArmoniKWorker}. Users typically do not
 * interact with this class directly:
 *
 * @see TaskProcessor
 * @see TaskContext
 * @see TaskContextFactory
 * @see ArmoniKWorker
 */
public class WorkerGrpc extends WorkerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(WorkerGrpc.class);

  private final AgentFutureStub agentStub;
  private final TaskProcessor taskProcessor;
  private final TaskContextFactory taskContextFactory;
  public final AtomicReference<ServingStatus> servingStatus;

  WorkerGrpc(AgentFutureStub agentStub, TaskProcessor taskProcessor, TaskContextFactory taskContextFactory) {
    this.agentStub = agentStub;
    this.taskProcessor = taskProcessor;
    this.taskContextFactory = taskContextFactory;
    this.servingStatus = new AtomicReference<>(SERVING);
  }

  WorkerGrpc(AgentFutureStub agentStub, TaskProcessor taskProcessor) {
    this(agentStub, taskProcessor, new DefaultTaskContextFactory());
  }

  /**
   * Processes a task assigned by the ArmoniK Agent.
   * <p>
   * This method implements the Worker service's {@code Process} RPC as defined in the ArmoniK API.
   * It orchestrates the complete task processing lifecycle:
   * </p>
   * <ol>
   *   <li>Sets serving status to {@link ServingStatus#NOT_SERVING}</li>
   *   <li>Creates a {@link TaskContext} using the factory</li>
   *   <li>Invokes the {@link TaskProcessor} with the context</li>
   *   <li>Converts the {@link TaskOutcome} to a gRPC {@link Output}</li>
   *   <li>Sends the response to the Agent</li>
   *   <li>Restores serving status to {@link ServingStatus#SERVING}</li>
   * </ol>
   *
   * <h4>Exception Handling</h4>
   * <p>
   * If the {@code taskProcessor} throws any exception:
   * </p>
   * <ul>
   *   <li>The exception is caught and its message is extracted</li>
   *   <li>If the message is {@code null}, {@code exception.toString()} is used instead</li>
   *   <li>A {@link TaskOutcome.Error} is created with the exception message</li>
   *   <li>The error is sent to the Agent as a normal response (not a gRPC error)</li>
   * </ul>
   *
   * <h4>Status Updates</h4>
   * <p>
   * The serving status is guaranteed to be restored to {@link ServingStatus#SERVING} even if
   * an exception occurs.
   * </p>
   *
   * @param request          the task processing request from the Agent containing task metadata,
   *                         payload, data dependencies, and expected outputs; never {@code null}
   * @param responseObserver the gRPC response observer for sending the processing result
   *                         back to the Agent; never {@code null}
   */
  @Override
  public void process(ProcessRequest request, StreamObserver<ProcessReply> responseObserver) {
    MDC.put("taskId", request.getTaskId());
    MDC.put("sessionId", request.getSessionId());

    long startTime = System.nanoTime();
    servingStatus.set(NOT_SERVING);

    try {
      logger.info("Received task processing request: payloadId={}, dataFolder={}", request.getPayloadId(), request.getDataFolder());
      var taskContext = taskContextFactory.create(agentStub, request);

      logger.info("Starting task processing");
      var outcome = taskProcessor.processTask(taskContext);

      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      if (outcome instanceof TaskOutcome.Success) {
        logger.info("Task processing completed successfully in {}ms", duration);
      } else if (outcome instanceof TaskOutcome.Error error) {
        logger.warn("Task processing completed with error in {}ms: {}", duration, error.message());
      }

      responseObserver.onNext(ProcessReply.newBuilder()
                                          .setOutput(toOutput(outcome))
                                          .build());
    } catch (Exception exception) {
      long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      String errorMessage = exception.getMessage() != null
        ? exception.getMessage()
        : exception.toString();

      logger.error("Task processing failed after {}ms with exception: {}",
        duration, errorMessage, exception);
      responseObserver.onNext(ProcessReply.newBuilder()
                                          .setOutput(toOutput(new TaskOutcome.Error(errorMessage)))
                                          .build());
    } finally {
      servingStatus.set(SERVING);
      responseObserver.onCompleted();
      MDC.clear();
    }
  }

  /**
   * Reports the health status of the Worker to the Agent.
   * <p>
   * This method implements the Worker service's {@code HealthCheck} RPC as defined in the
   * ArmoniK API. The Agent periodically calls this method to monitor Worker availability
   * and determine whether the Worker is ready to accept new tasks.
   * </p>
   *
   * <h4>Serving Status</h4>
   * <p>
   * The returned status indicates:
   * </p>
   * <ul>
   *   <li>{@link ServingStatus#SERVING}: Worker is idle and can accept a new task</li>
   *   <li>{@link ServingStatus#NOT_SERVING}: Worker is busy processing a task</li>
   * </ul>
   *
   * <h4>Agent Behavior</h4>
   * <p>
   * The Agent uses this status to:
   * </p>
   * <ul>
   *   <li>Verify the Worker is alive before assigning tasks</li>
   *   <li>Avoid sending tasks to busy Workers</li>
   *   <li>Detect and restart unresponsive Workers</li>
   * </ul>
   *
   * @param request          empty request (health checks have no parameters)
   * @param responseObserver the gRPC response observer for sending the health status
   *                         back to the Agent; never {@code null}
   */
  @Override
  public void healthCheck(Empty request, StreamObserver<HealthCheckReply> responseObserver) {
    responseObserver.onNext(HealthCheckReply.newBuilder()
                                            .setStatus(servingStatus.get())
                                            .build());
    responseObserver.onCompleted();
  }

  private Output toOutput(TaskOutcome outcome) {
    var outputBuilder = Output.newBuilder();
    if (outcome instanceof TaskOutcome.Success) {
      outputBuilder.setOk(Empty.newBuilder().build());
    } else if (outcome instanceof TaskOutcome.Error error) {
      outputBuilder.setError(Error.newBuilder().setDetails(error.message()).build());
    }
    return outputBuilder.build();
  }
}
