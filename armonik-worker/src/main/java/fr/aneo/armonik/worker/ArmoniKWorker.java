package fr.aneo.armonik.worker;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ArmoniKWorker {
  private static final Logger logger = LoggerFactory.getLogger(ArmoniKWorker.class);

  private final TaskProcessor taskProcessor;
  private Server server;
  private InetSocketAddress address;

  public ArmoniKWorker(TaskProcessor taskProcessor) {
    this.taskProcessor = taskProcessor;
  }

  public void start() throws IOException {
    address = resolveAddress();
    server = NettyServerBuilder.forAddress(address)
                               .permitKeepAliveWithoutCalls(true)
                               .permitKeepAliveTime(30, SECONDS)
                               .keepAliveTime(30, SECONDS)
                               .keepAliveTimeout(10, SECONDS)
                               .maxInboundMetadataSize(1024 * 1024)
                               .maxInboundMessageSize(8 * 1024 * 1024)
                               .addService(new WorkerGrpc(taskProcessor))
                               .build();

    server.start();
    logger.info("gRPC Worker started on {}:{}", address.getHostString(), address.getPort());

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
    return address;
  }

  private static InetSocketAddress resolveAddress() {
    var rawAddress = System.getenv("ComputePlane__WorkerChannel__Address");

    if (rawAddress == null || rawAddress.isBlank()) {
      logger.warn("Environment variable ComputePlane__WorkerChannel__Address is not set. Falling back to default 0.0.0.0:8080");
      return new InetSocketAddress("0.0.0.0", 8080);
    }


    var parts = rawAddress.split(":");
    if (parts.length != 2)
      throw new IllegalArgumentException("Invalid worker address format. Expected 'ip:port' but got: " + rawAddress);

    String host = parts[0].trim();
    String portAsString = parts[1].trim();

    validateHost(host, rawAddress);
    int port = parseAndValidatePort(portAsString, rawAddress);

    logger.info("Configured worker address from environment: {}:{}", host, port);
    return new InetSocketAddress(host, port);
  }

  private static void validateHost(String host, String rawAddress) {
    String[] octets = host.split("\\.");
    if (octets.length != 4)
      throw new IllegalArgumentException("Invalid IPv4 '" + host + "' in worker address: " + rawAddress);

    Arrays.stream(octets).forEach(octet -> {
      if (!octet.chars().allMatch(Character::isDigit))
        throw new IllegalArgumentException("Invalid IPv4 '" + host + "' in worker address: " + rawAddress);
      int value = Integer.parseInt(octet);
      if (value < 0 || value > 255)
        throw new IllegalArgumentException("Invalid IPv4 '" + host + "' in worker address: " + rawAddress);
    });
  }

  private static int parseAndValidatePort(String portStr, String rawAddress) {
    try {
      int port = Integer.parseInt(portStr);
      if (port < 1 || port > 65535)
        throw new IllegalArgumentException("Invalid port '" + port + "' in worker address: " + rawAddress +
          ". Port must be between 1 and 65535.");
      return port;
    } catch (NumberFormatException e) {
      logger.error("Invalid port '{}' in worker address '{}'. Must be numeric.", portStr, rawAddress);
      throw new IllegalArgumentException("Invalid port '" + portStr + "' in worker address: " + rawAddress +
        ". Port must be numeric.", e);
    }
  }
}
