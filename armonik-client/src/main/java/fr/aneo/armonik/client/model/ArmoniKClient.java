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

import fr.aneo.armonik.api.grpc.v1.sessions.SessionsGrpc;
import fr.aneo.armonik.client.GrpcChannelBuilder;
import fr.aneo.armonik.client.PemClientCertificate;
import fr.aneo.armonik.client.Pkcs12ClientCertificate;
import fr.aneo.armonik.client.definition.SessionDefinition;
import io.grpc.ManagedChannel;

import static fr.aneo.armonik.client.internal.grpc.mappers.SessionMapper.toCreateSessionRequest;
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

    this.channelPool = ManagedChannelPool.create(50, () -> createChannel(armoniKConfig));
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
    var id = createSession(sessionDefinition);

    return new SessionHandle(id, sessionDefinition, channelPool);
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
    var channelBuilder = GrpcChannelBuilder.forEndpoint(armoniKConfig.endpoint());

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
      channelBuilder.withClientCertificate(Pkcs12ClientCertificate.of(armoniKConfig.clientP12(), armoniKConfig.clientKeyPem()));
    }

    return channelBuilder.build();
  }
}
