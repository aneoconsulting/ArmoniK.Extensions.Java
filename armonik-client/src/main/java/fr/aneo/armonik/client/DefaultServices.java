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

import fr.aneo.armonik.client.blob.BlobService;
import fr.aneo.armonik.client.blob.DefaultBlobService;
import fr.aneo.armonik.client.session.DefaultSessionService;
import fr.aneo.armonik.client.session.SessionService;
import fr.aneo.armonik.client.task.DefaultTaskService;
import fr.aneo.armonik.client.task.TaskService;
import io.grpc.ManagedChannel;

import static java.util.Objects.*;

/**
 * Default {@link Services} implementation wiring gRPC stubs to client service facades.
 */
final class DefaultServices implements Services {
  private final SessionService sessionService;
  private final BlobService blobService;
  private final TaskService taskService;

  DefaultServices(ArmoniKConnectionConfig armoniKConnectionConfig) {
    requireNonNull(armoniKConnectionConfig, "connectionConfiguration must not be null");

    var channel = buildChannel(armoniKConnectionConfig);
    this.sessionService = new DefaultSessionService(channel);
    this.blobService = new DefaultBlobService(channel);
    this.taskService = new DefaultTaskService(channel);
  }

  @Override
  public SessionService sessions() {
    return sessionService;
  }

  @Override
  public BlobService blobs() {
    return blobService;
  }

  @Override
  public TaskService tasks() {
    return taskService;
  }

  private ManagedChannel buildChannel(ArmoniKConnectionConfig connectionConfiguration) {
    var channelBuilder = GrpcChannelBuilder.forEndpoint(connectionConfiguration.endpoint());
    if (!connectionConfiguration.sslValidation()) channelBuilder.withUnsecureConnection();

    return channelBuilder.build();
  }
}
