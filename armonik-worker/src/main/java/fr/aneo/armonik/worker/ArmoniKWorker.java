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

import static java.util.concurrent.TimeUnit.SECONDS;

public class ArmoniKWorker {
  private static final Logger logger = LoggerFactory.getLogger(ArmoniKWorker.class);

  private final TaskProcessor taskProcessor;
  private Server server;
  private InetSocketAddress workerAddress;
  private ManagedChannel agentChannel;

  public ArmoniKWorker(TaskProcessor taskProcessor) {
    this.taskProcessor = taskProcessor;
  }

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

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      logger.info("Blocking until gRPC worker shutdown...");
      server.awaitTermination();
      logger.info("gRPC worker terminated.");
    } else {
      logger.warn("blockUntilShutdown() called but server is null (not started).");
    }
  }

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
