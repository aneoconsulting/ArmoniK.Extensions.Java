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

import fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;

/**
 * Factory interface for creating {@link TaskHandler} instances from Agent requests.
 * <p>
 * A {@code TaskHandlerFactory} is responsible for constructing a {@link TaskHandler} that
 * provides access to task inputs, outputs, and processing capabilities. The factory pattern
 * allows for customization of task handler creation, particularly useful for testing or
 * specialized task processing scenarios.
 * </p>
 *
 * <h2>Purpose</h2>
 * <p>
 * This factory abstraction enables:
 * </p>
 * <ul>
 *   <li><strong>Testability</strong>: Mock or stub task handlers can be injected for unit testing</li>
 *   <li><strong>Customization</strong>: Alternative task handler implementations can be provided
 *       without modifying {@link WorkerGrpc}</li>
 *   <li><strong>Dependency Injection</strong>: The factory can be configured externally and
 *       injected into the Worker gRPC service</li>
 * </ul>
 *
 * <h2>Default Implementation</h2>
 * <p>
 * The default factory implementation uses the static factory method:
 * </p>
 * <pre>{@code
 * TaskHandlerFactory defaultFactory = TaskHandler::from;
 * }</pre>
 * <p>
 * This is the recommended implementation for production use and is automatically used by
 * {@link WorkerGrpc} when constructed without an explicit factory.
 * </p>
 *
 * <h2>Custom Implementation Example</h2>
 * <p>
 * For testing or specialized scenarios, custom factories can be provided:
 * </p>
 * <pre>{@code
 * // Testing with a mock handler
 * TaskHandlerFactory mockFactory = (agentStub, request) -> {
 *     var mockHandler = mock(TaskHandler.class);
 *     when(mockHandler.getInput("data")).thenReturn(testInput);
 *     return mockHandler;
 * };
 *
 * // Custom handler with additional validation
 * TaskHandlerFactory validatingFactory = (agentStub, request) -> {
 *     validateRequest(request);
 *     return TaskHandler.from(agentStub, request);
 * };
 * }</pre>
 *
 * <h2>Lambda Expression Support</h2>
 * <p>
 * As a functional interface, {@code TaskHandlerFactory} can be implemented using lambda
 * expressions or method references:
 * </p>
 * <pre>{@code
 * TaskHandlerFactory factory = TaskHandler::from;
 * TaskHandlerFactory customFactory = (stub, req) -> new CustomTaskHandler(stub, req);
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Implementations should be stateless and thread-safe if the same factory instance is
 * reused across multiple Worker instances. However, since {@link ArmoniKWorker} creates
 * a single Worker instance per container, thread safety is not typically required.
 * </p>
 *
 * @see TaskHandler
 * @see WorkerGrpc
 * @see WorkerCommon.ProcessRequest
 */
@FunctionalInterface
public interface TaskHandlerFactory {

  /**
   * Creates a new {@link TaskHandler} for processing a task request.
   * <p>
   * This method is called by {@link WorkerGrpc} for each task received from the Agent.
   * The implementation should construct a task handler that provides access to:
   * </p>
   * <ul>
   *   <li>Task metadata (session ID, task ID, communication token, etc.)</li>
   *   <li>Input data (payload and data dependencies)</li>
   *   <li>Output destinations (expected output keys)</li>
   *   <li>Agent communication channel for submitting subtasks and results</li>
   * </ul>
   *
   * @param agentStub the gRPC stub for communicating with the ArmoniK Agent;
   *                  used for submitting subtasks and notifying result availability;
   *                  never {@code null}
   * @param request   the task processing request from the Agent containing all task metadata
   *                  and references to input/output data; never {@code null}
   * @return a fully initialized task handler ready to process the task; must not be {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   * @throws ArmoniKException     if task handler creation fails due to invalid request data,
   *                              missing files, or I/O errors
   * @throws RuntimeException     if any unexpected error occurs during creation
   */
  TaskHandler create(AgentFutureStub agentStub, WorkerCommon.ProcessRequest request);
}
