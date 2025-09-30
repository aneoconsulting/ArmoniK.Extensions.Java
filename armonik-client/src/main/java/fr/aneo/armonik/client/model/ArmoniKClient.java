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
 *
 * @see SessionHandle
 * @see SessionDefinition
 * @see ArmoniKConnectionConfig
 */
public class ArmoniKClient {
  private final ManagedChannel channel;


  /**
   * Constructs a new ArmoniK client with the specified connection configuration.
   * <p>
   * This constructor establishes a gRPC channel to the ArmoniK cluster using the provided
   * connection settings. The client will use this channel for all later operations,
   * including session creation and management.
   *
   * @param connectionConfiguration the connection configuration specifying how to connect to the ArmoniK cluster
   * @throws NullPointerException if connectionConfiguration is null
   * @throws IllegalArgumentException if the connection configuration is invalid
   * @see ArmoniKConnectionConfig
   */
  public ArmoniKClient(ArmoniKConnectionConfig connectionConfiguration) {
    channel = buildChannel(connectionConfiguration);
  }

  /**
   * Package-private constructor for creating a client with an existing gRPC channel.
   * <p>
   * This constructor is primarily used for testing or advanced scenarios where the gRPC channel
   * is managed externally.
   *
   * @param channel the managed gRPC channel to use for communication with the ArmoniK cluster
   * @throws NullPointerException if channel is null
   */
  ArmoniKClient(ManagedChannel channel) {
    this.channel = channel;
  }


  /**
   * Returns the gRPC channel used by this client for communication with the ArmoniK cluster.
   * <p>
   * This method provides access to the underlying gRPC channel, which may be useful for
   * advanced scenarios or debugging purposes. The returned channel should not be closed
   * directly as it is managed by this client instance.
   *
   * @return the managed gRPC channel used for cluster communication
   */
  public ManagedChannel channel() {
    return channel;
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
   *                         task configuration defaults, and optional session identification
   * @return a SessionHandle for managing the session and submitting tasks
   * @throws NullPointerException if sessionDefinition is null
   * @throws RuntimeException if session creation fails due to cluster communication issues
   * @see SessionHandle
   * @see SessionDefinition
   */
  public SessionHandle openSession(SessionDefinition sessionDefinition) {
    var id = createSession(sessionDefinition);

    return new SessionHandle(id, sessionDefinition, channel);
  }

  private ManagedChannel buildChannel(ArmoniKConnectionConfig connectionConfiguration) {
    var channelBuilder = GrpcChannelBuilder.forEndpoint(connectionConfiguration.endpoint());
    if (!connectionConfiguration.sslValidation()) channelBuilder.withUnsecureConnection();

    return channelBuilder.build();
  }


  private SessionInfo createSession(SessionDefinition definition) {
    requireNonNull(definition);

    var sessionReply = SessionsGrpc.newBlockingStub(channel).createSession(toCreateSessionRequest(definition));

    return new SessionInfo(
      SessionId.from(sessionReply.getSessionId()),
      definition.partitionIds(),
      definition.taskConfiguration()
    );
  }
}
