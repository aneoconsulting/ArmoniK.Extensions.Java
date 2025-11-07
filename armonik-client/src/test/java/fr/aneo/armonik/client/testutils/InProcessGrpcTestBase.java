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
package fr.aneo.armonik.client.testutils;

import fr.aneo.armonik.client.ManagedChannelPool;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.List;

/**
 * Base class to spin up an in-process gRPC server/channel for tests.
 */
public abstract class InProcessGrpcTestBase {
  protected ManagedChannelPool channelPool;
  private Server server;

  /** Subclasses provide the services they want to add to the server. */
  protected abstract List<BindableService> services();

  @BeforeEach
  void startServer() throws IOException {
    String name = InProcessServerBuilder.generateName();
    InProcessServerBuilder serverBuilder = InProcessServerBuilder.forName(name).directExecutor();
    services().forEach(serverBuilder::addService);
    server = serverBuilder.build().start();
    channelPool = ManagedChannelPool.createUnbounded(() -> InProcessChannelBuilder.forName(name).directExecutor().build());
  }

  @AfterEach
  void stopServer() throws InterruptedException {
    if (channelPool != null) {
      channelPool.close();
    }
    if (server != null) {
      server.shutdownNow().awaitTermination();
    }
  }
}
