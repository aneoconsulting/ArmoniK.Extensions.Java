/*
 * Copyright (C) 2025 ANEO (armonik@aneo.fr)
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
package fr.aneo.armonik.client.it;

import fr.aneo.armonik.api.grpc.v1.sessions.SessionsCommon;
import fr.aneo.armonik.api.grpc.v1.sessions.SessionsGrpc;
import fr.aneo.armonik.client.definition.SessionDefinition;
import io.grpc.Attributes;
import io.grpc.ServerTransportFilter;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * The client's keep-alive pings are accepted by a server that enforces gRPC's default keep-alive
 * policy: at most one ping every 5 minutes, none on a connection without calls. This is the default
 * of a grpc-java server, started here in the test JVM: no cluster is needed.
 * <p>
 * Expected: an idle client keeps its connection; the server never closes it with {@code too_many_pings}.
 */
@Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class KeepAliveIT {

  @Test
  @Disabled("Fails while the client pings every 30 s, even without calls: https://github.com/aneoconsulting/ArmoniK.Extensions.Java/issues/76")
  void an_idle_client_keeps_its_connection_to_a_server_with_the_default_keep_alive_policy() throws Exception {
    var closedByServer = new AtomicInteger();
    var server = NettyServerBuilder.forPort(0)
                                   .addService(new SessionsGrpc.SessionsImplBase() {
                                     @Override
                                     public void createSession(SessionsCommon.CreateSessionRequest request,
                                                               StreamObserver<SessionsCommon.CreateSessionReply> responseObserver) {
                                       responseObserver.onNext(SessionsCommon.CreateSessionReply.newBuilder().setSessionId("keep-alive").build());
                                       responseObserver.onCompleted();
                                     }
                                   })
                                   .addTransportFilter(new ServerTransportFilter() {
                                     @Override
                                     public void transportTerminated(Attributes transportAttrs) {
                                       closedByServer.incrementAndGet();
                                     }
                                   })
                                   .build()
                                   .start();
    try (var client = newClient("http://localhost:" + server.getPort(), builder -> {
    })) {
      call(Duration.ofSeconds(30), "openSession on a local server", () -> client.openSession(new SessionDefinition(Set.of(PARTITION))));
      // A 30 s keep-alive sends its second ping after 60 s of idleness, when the server gives up on it
      sleep(90_000);
      metric("KeepAliveIT", "connections_closed_by_server_after_90s_idle", closedByServer.get());
      assertThat(closedByServer.get()).as("connections the server closed while the client was idle").isZero();
    } finally {
      server.shutdownNow();
    }
  }
}
