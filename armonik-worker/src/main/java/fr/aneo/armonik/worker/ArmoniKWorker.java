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

import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import fr.aneo.armonik.worker.internal.AddressResolver;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Entry point for running an ArmoniK Worker that processes tasks submitted to ArmoniK's Control Plane.
 * <p>
 * An ArmoniK Worker is a gRPC server that implements the Worker service as defined in the ArmoniK API.
 * It receives task execution requests from the ArmoniK Agent, delegates processing to a {@link TaskProcessor},
 * and reports task outcomes back to the Agent.
 *
 * <h2>Architecture</h2>
 * <p>
 * The Worker operates within the ArmoniK Compute Plane and interacts with two main components:
 * <ul>
 *   <li><strong>Agent</strong>: Supervises worker processes and manages task/result submission from workers</li>
 *   <li><strong>Control Plane</strong>: Orchestrates task scheduling and lifecycle management</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <p>
 * The Worker requires the following environment variables to be set:
 * <dl>
 *   <dt><code>ComputePlane__WorkerChannel__Address</code></dt>
 *   <dd>Address where the Worker gRPC server listens (format: "host:port").
 *       If not set, defaults to "0.0.0.0:8080".</dd>
 *
 *   <dt><code>ComputePlane__AgentChannel__Address</code></dt>
 *   <dd>Address of the ArmoniK Agent to connect to (format: "host:port").
 *       <strong>Required</strong> - throws {@link IllegalStateException} if missing.</dd>
 * </dl>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * TaskProcessor processor = taskHandler -> {
 *     // Process task logic here
 *     return new TaskOutcome.Success();
 * };
 *
 * ArmoniKWorker worker = new ArmoniKWorker(processor);
 * worker.start();
 * worker.blockUntilShutdown();
 * }</pre>
 *
 * <h2>Health Monitoring</h2>
 * <p>
 * The Worker implements a health check mechanism that is regularly polled by the Agent to verify
 * the Worker's operational status. The Agent uses this health check to ensure the Worker is alive
 * and responsive before dispatching tasks to it.
 *
 * <h2>Lifecycle</h2>
 * <p>
 * The Worker follows this lifecycle:
 * <ol>
 *   <li><strong>Construction</strong>: Create instance with a {@link TaskProcessor}</li>
 *   <li><strong>Start</strong>: Call {@link #start()} to initialize gRPC server and agent connection</li>
 *   <li><strong>Processing</strong>: Worker receives and processes tasks until shutdown</li>
 *   <li><strong>Shutdown</strong>: Graceful shutdown via {@link #shutdown()} or JVM shutdown hook</li>
 * </ol>
 *
 * <h2>Graceful Shutdown</h2>
 * <p>
 * A JVM shutdown hook is automatically registered during {@link #start()} to ensure graceful shutdown.
 * The Worker attempts to stop accepting new requests and complete in-flight tasks within 30 seconds,
 * followed by a forced shutdown with an additional 5-second timeout if needed. The agent channel
 * is also properly closed during shutdown.
 *
 * @see TaskProcessor
 * @see TaskHandler
 * @see <a href="https://armonik.readthedocs.io/en/latest/">ArmoniK Documentation</a>
 */
public class ArmoniKWorker {
  private static final Logger logger = LoggerFactory.getLogger(ArmoniKWorker.class);

  private final TaskProcessor taskProcessor;
  private Server server;
  private InetSocketAddress workerAddress;
  private ManagedChannel agentChannel;

  /**
   * Creates a new ArmoniK Worker with the specified task processor.
   * <p>
   * Only one Worker instance should be created per container.
   * </p>
   *
   * @param taskProcessor the processor responsible for executing tasks; must not be {@code null}
   * @throws NullPointerException if {@code taskProcessor} is {@code null}
   */
  public ArmoniKWorker(TaskProcessor taskProcessor) {
    this.taskProcessor = requireNonNull(taskProcessor);
  }

  /**
   * Starts the Worker gRPC server and establishes connection to the ArmoniK Agent.
   * <p>
   * This method should be called only once per Worker instance.
   * </p>
   * <p>
   * This method:
   * </p>
   * <ul>
   *   <li>Resolves the Worker listening address from the {@code ComputePlane__WorkerChannel__Address}
   *       environment variable (defaults to 0.0.0.0:8080 if not set)</li>
   *   <li>Establishes a gRPC channel to the Agent using {@code ComputePlane__AgentChannel__Address}</li>
   *   <li>Starts the gRPC server with configured keep-alive settings and message size limits</li>
   *   <li>Registers a JVM shutdown hook for graceful termination</li>
   * </ul>
   *
   * @throws IOException           if the server fails to bind to the specified address
   * @throws IllegalStateException if {@code ComputePlane__AgentChannel__Address} environment variable is not set
   */
  public void start() throws IOException {
    workerAddress = AddressResolver.resolve(System.getenv("ComputePlane__WorkerChannel__Address"))
                                   .orElseGet(() -> {
                                     logger.warn("Environment variable ComputePlane__WorkerChannel__Address is not set. Falling back to default 0.0.0.0:8080");
                                     return new InetSocketAddress("0.0.0.0", 8080);
                                   });

    agentChannel = buildAgentChannel();

    server = NettyServerBuilder.forAddress(workerAddress)
                               .permitKeepAliveWithoutCalls(true)
                               .permitKeepAliveTime(30, SECONDS)
                               .keepAliveTime(30, SECONDS)
                               .keepAliveTimeout(10, SECONDS)
                               .maxInboundMetadataSize(1024 * 1024)
                               .maxInboundMessageSize(8 * 1024 * 1024)
                               .addService(new WorkerGrpc(AgentGrpc.newFutureStub(agentChannel), taskProcessor))
                               .build();

    server.start();
    logger.info("gRPC Worker started on {}:{}", workerAddress.getHostString(), workerAddress.getPort());

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        shutdown();
      } catch (InterruptedException e) {
        logger.warn("Server shutdown interrupted");
        Thread.currentThread().interrupt();
      }
    }));
  }

  /**
   * Initiates graceful shutdown of the Worker gRPC server and closes the agent channel.
   * <p>
   * The shutdown process:
   * <ol>
   *   <li>Stops accepting new RPC requests on the Worker server</li>
   *   <li>Waits up to 30 seconds for in-flight requests to complete</li>
   *   <li>Forces shutdown if graceful termination times out</li>
   *   <li>Waits an additional 5 seconds for a forced shutdown to complete</li>
   *   <li>Shuts down the agent channel gracefully (30csecond timeout)</li>
   *   <li>Forces agent channel shutdown if graceful termination times out (5 seconds timeout)</li>
   * </ol>
   * <p>
   * This method is safe to call multiple times. Subsequent calls after the first shutdown have no effect.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting for shutdown
   */
  public void shutdown() throws InterruptedException {
    if (server != null) {
      logger.info("Initiating graceful shutdown of gRPC worker...");
      server.shutdown();
      if (!server.awaitTermination(30, SECONDS)) {
        logger.warn("Graceful shutdown timed out. Forcing shutdown...");
        server.shutdownNow();
        server.awaitTermination(5, SECONDS);
      }
      logger.info("gRPC worker stopped successfully.");
    } else {
      logger.info("Shutdown requested but server was not running.");
    }

    if (agentChannel != null) {
      logger.info("Shutting down agent channel...");
      agentChannel.shutdown();
      if (!agentChannel.awaitTermination(30, SECONDS)) {
        logger.warn("Agent channel graceful shutdown timed out. Forcing shutdown...");
        agentChannel.shutdownNow();
        agentChannel.awaitTermination(5, SECONDS);
      }
      logger.info("Agent channel closed successfully.");
    }
  }

  /**
   * Blocks the current thread until the Worker server terminates.
   * <p>
   * This method is typically used in the main thread to keep the application alive while the Worker
   * processes tasks. The blocking continues until:
   * <ul>
   *   <li>{@link #shutdown()} is called from another thread</li>
   *   <li>The JVM shutdown hook triggers (e.g., SIGTERM, SIGINT)</li>
   *   <li>The server encounters a fatal error</li>
   * </ul>
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      logger.info("Blocking until gRPC worker shutdown...");
      server.awaitTermination();
      logger.info("gRPC worker terminated.");
    } else {
      logger.warn("blockUntilShutdown() called but server is null (not started).");
    }
  }

  /**
   * Returns the network address where the Worker gRPC server is listening.
   * <p>
   * This address is resolved during {@link #start()} and reflects the actual binding address,
   * which may differ from the configured address if wildcard binding (e.g., 0.0.0.0) was used.
   * </p>
   *
   * @return the Worker's listening address, or {@code null} if {@link #start()} has not been called
   */
  public InetSocketAddress address() {
    return workerAddress;
  }

  private ManagedChannel buildAgentChannel() {
    var agentAddress = AddressResolver.resolve(System.getenv("ComputePlane__AgentChannel__Address"))
                                      .orElseThrow(() -> new IllegalStateException("Environment variable ComputePlane__AgentChannel__Address is not set"));

    return NettyChannelBuilder.forAddress(agentAddress.getHostString(), agentAddress.getPort())
                              .usePlaintext()
                              .keepAliveTime(20, SECONDS)
                              .keepAliveTimeout(10, SECONDS)
                              .keepAliveWithoutCalls(true)
                              .enableRetry()
                              .build();
  }
}
