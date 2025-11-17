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
package fr.aneo.armonik.client;

import fr.aneo.armonik.api.grpc.v1.sessions.SessionsGrpc;
import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.exception.ArmoniKException;
import io.grpc.ManagedChannel;

import java.time.Duration;
import java.util.HashSet;

import static fr.aneo.armonik.client.internal.grpc.mappers.SessionMapper.toCreateSessionRequest;
import static fr.aneo.armonik.client.internal.grpc.mappers.SessionMapper.toGetSessionRequest;
import static fr.aneo.armonik.client.internal.grpc.mappers.TaskMapper.toTaskConfiguration;
import static java.util.Objects.requireNonNull;

/**
 * The main entry point for interacting with the ArmoniK distributed computing platform.
 * <p>
 * This client provides the primary interface for creating and connecting to sessions within the ArmoniK cluster.
 * <p>
 * The client is responsible for establishing gRPC connections to the ArmoniK cluster and serves as a factory
 * for session management operations. Once created, sessions are managed through {@link SessionHandle} instances
 * that handle their own lifecycle and operations.
 * <p>
 * <strong>Resource Management:</strong> This client implements {@link AutoCloseable} and should be used
 * with try-with-resources to ensure proper cleanup of gRPC channels:
 * <pre>{@code
 * var config = ArmoniKConfig.builder()
 *   .endpoint("https://armonik.example.com:443")
 *   .build();
 *
 * try (var client = new ArmoniKClient(config)) {
 *   var session = client.openSession(sessionDefinition);
 *   // ... use session
 * } // Channels automatically closed
 * }</pre>
 *
 * @see SessionHandle
 * @see SessionDefinition
 * @see ArmoniKConfig
 */
public class ArmoniKClient implements AutoCloseable {
  private final ChannelPool channelPool;
  final int DEFAULT_CHANNEL_POOL_SIZE = 50;


  /**
   * Constructs a new ArmoniK client with the specified connection configuration.
   * <p>
   * This constructor establishes a gRPC channel to the ArmoniK cluster using the provided
   * connection settings. The client will use this channel for all later operations,
   * including session creation and management.
   *
   * @param armoniKConfig the connection configuration specifying how to connect to the ArmoniK cluster
   * @throws NullPointerException     if connectionConfiguration is null
   * @throws IllegalArgumentException if the connection configuration is invalid
   * @see ArmoniKConfig
   */
  public ArmoniKClient(ArmoniKConfig armoniKConfig) {
    requireNonNull(armoniKConfig, "armoniKConfig must not be null");

    this.channelPool = ManagedChannelPool.create(
      DEFAULT_CHANNEL_POOL_SIZE,
      armoniKConfig.retryPolicy(),
      () -> createChannel(armoniKConfig)
    );
  }

  ArmoniKClient(ChannelPool channelPool) {
    this.channelPool = channelPool;
  }


  /**
   * Opens a new session in the ArmoniK cluster or connects to an existing one.
   * <p>
   * This method creates a new session based on the provided session definition. The returned
   * {@link SessionHandle} provides access to session operations including task submission and
   * lifecycle management.
   * <p>
   * Sessions serve as logical workspaces that scope task submissions and blob management within
   * the ArmoniK cluster. All tasks submitted within a session share the same execution context
   * and resource allocation settings.
   *
   * @param sessionDefinition the definition specifying session parameters such as partition,
   *                          task configuration defaults, and optional session identification
   * @return a SessionHandle for managing the session and submitting tasks
   * @throws NullPointerException if sessionDefinition is null
   * @throws RuntimeException     if session creation fails due to cluster communication issues
   * @see SessionHandle
   * @see SessionDefinition
   */
  public SessionHandle openSession(SessionDefinition sessionDefinition) {
    requireNonNull(sessionDefinition, "sessionDefinition must not be null");

    var sessionInfo = createSession(sessionDefinition);
    return new SessionHandle(sessionInfo, sessionDefinition.outputListener(), channelPool);
  }

  /**
   * Connects to an existing session in the ArmoniK cluster.
   * <p>
   * This method retrieves the session metadata associated with the given {@link SessionId}
   * and returns a {@link SessionHandle} that can be used to submit tasks and monitor
   * output blob completions
   * <p>
   * If a {@link BlobCompletionListener} is provided, the returned {@code SessionHandle}
   * will automatically start listening for output blob completion events for this
   * session.
   *
   * <p><strong>Error Handling:</strong>
   * If the session does not exist, this method throws an {@link ArmoniKException}
   *
   * @param sessionId
   *        the identifier of the existing session to connect to
   * @param outputListener
   *        an optional listener for output blob completion events;
   *        if {@code null}, no listener is registered
   *
   * @return a {@link SessionHandle} bound to the existing session
   *
   * @throws NullPointerException
   *         if {@code sessionId} is {@code null}
   * @throws ArmoniKException
   *         if the session does not exist, or if a communication error occurs
   *
   * @see SessionHandle
   * @see BlobCompletionListener
   */
  public SessionHandle getSession(SessionId sessionId, BlobCompletionListener outputListener) {
    requireNonNull(sessionId, "sessionId must not be null");

    try {
      var sessionResponse = channelPool.execute(
        channel -> SessionsGrpc.newBlockingStub(channel).getSession(toGetSessionRequest(sessionId))
      );
      if (!sessionResponse.hasSession()) throw new ArmoniKException("Session not found");

      var sessionInfo = new SessionInfo(
        sessionId,
        new HashSet<>(sessionResponse.getSession().getPartitionIdsList()),
        toTaskConfiguration(sessionResponse.getSession().getOptions())
      );

      return new SessionHandle(sessionInfo, outputListener, channelPool);
    } catch (Exception e) {
      throw new ArmoniKException(e.getMessage(), e);
    }
  }

  /**
   * Connects to an existing session in the ArmoniK cluster without registering
   * an output listener.
   * <p>
   * This is a convenience overload equivalent to calling:
   * <pre>{@code
   * getSession(sessionId, null);
   * }</pre>
   *
   * <p><strong>Error Handling:</strong>
   * If the session does not exist, this method throws an {@link ArmoniKException}.
   *
   * @param sessionId
   *        the identifier of the existing session to connect to
   *
   * @return a {@link SessionHandle} for interacting with the session
   *
   * @throws NullPointerException
   *         if {@code sessionId} is {@code null}
   * @throws ArmoniKException
   *         if the session does not exist, or if a communication error occurs
   *
   * @see #getSession(SessionId, BlobCompletionListener)
   */
  public SessionHandle getSession(SessionId sessionId) {
    return getSession(sessionId, null);
  }

  /**
   * Closes this client and releases all associated resources.
   * <p>
   * This method initiates a graceful shutdown of the channel pool. All channels will be
   * closed, and any in-flight operations will be allowed to complete within the shutdown
   * timeout period. If interrupted during shutdown, immediate shutdown is forced.
   * <p>
   * After calling this method, the client should not be used for any operations.
   */
  @Override
  public void close() {
    channelPool.close();
  }

  private SessionInfo createSession(SessionDefinition definition) {
    requireNonNull(definition);

    var sessionReply = channelPool.execute(channel ->
      SessionsGrpc.newBlockingStub(channel).createSession(toCreateSessionRequest(definition))
    );

    return new SessionInfo(
      SessionId.from(sessionReply.getSessionId()),
      definition.partitionIds(),
      definition.taskConfiguration()
    );
  }

  private ManagedChannel createChannel(ArmoniKConfig armoniKConfig) {
    var channelBuilder = GrpcChannelBuilder.forEndpoint(armoniKConfig.endpoint())
                                           .withRetry(armoniKConfig.retryPolicy())
                                           .withIdleTimeout(Duration.ofMinutes(5))
                                           .withKeepAlive(Duration.ofSeconds(30), Duration.ofSeconds(30));

    if (!armoniKConfig.sslValidation()) {
      channelBuilder.withUnsecureConnection();
    }

    if (armoniKConfig.caCertPem() != null && !armoniKConfig.caCertPem().isBlank()) {
      channelBuilder.withCaPem(armoniKConfig.caCertPem());
    }

    if (armoniKConfig.clientCertPem() != null && armoniKConfig.clientKeyPem() != null) {
      channelBuilder.withClientCertificate(
        PemClientCertificate.of(armoniKConfig.clientCertPem(), armoniKConfig.clientKeyPem())
      );
    }

    if (armoniKConfig.clientP12() != null) {
      channelBuilder.withClientCertificate(Pkcs12ClientCertificate.of(armoniKConfig.clientP12(), armoniKConfig.clientP12Password()));
    }

    return channelBuilder.build();
  }
}
