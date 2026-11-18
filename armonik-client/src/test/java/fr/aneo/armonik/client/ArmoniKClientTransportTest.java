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

import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.testutils.SessionsGrpcMock;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Connects clients to real gRPC servers, one over TLS with a self-signed certificate issued for
 * another host name, so that it passes neither chain nor host name validation, and one without TLS.
 */
class ArmoniKClientTransportTest {
  private static SelfSignedCertificate certificate;
  private static Server tlsServer;
  private static Server plaintextServer;

  @BeforeAll
  static void startServers() throws Exception {
    certificate = new SelfSignedCertificate("untrusted.example");
    tlsServer = NettyServerBuilder.forPort(0)
                                  .sslContext(GrpcSslContexts.forServer(certificate.certificate(), certificate.privateKey()).build())
                                  .addService(new SessionsGrpcMock())
                                  .build()
                                  .start();
    plaintextServer = NettyServerBuilder.forPort(0)
                                        .addService(new SessionsGrpcMock())
                                        .build()
                                        .start();
  }

  @AfterAll
  static void stopServers() {
    if (tlsServer != null) tlsServer.shutdownNow();
    if (plaintextServer != null) plaintextServer.shutdownNow();
    if (certificate != null) certificate.delete();
  }

  @Test
  @DisplayName("should connect over TLS to a server whose certificate is not validated")
  @Timeout(30)
  void should_connect_over_tls_without_ssl_validation() {
    var config = ArmoniKConfig.builder()
                              .endpoint("https://localhost:" + tlsServer.getPort())
                              .withoutSslValidation()
                              .build();

    try (var client = new ArmoniKClient(config)) {
      assertThat(client.openSession(sessionDefinition()).sessionInfo().id()).isNotNull();
    }
  }

  @Test
  @DisplayName("should refuse the same server when its certificate is validated")
  @Timeout(30)
  void should_refuse_untrusted_server_certificate_by_default() {
    var config = ArmoniKConfig.builder()
                              .endpoint("https://localhost:" + tlsServer.getPort())
                              .build();

    try (var client = new ArmoniKClient(config)) {
      assertThatThrownBy(() -> client.openSession(sessionDefinition())).isInstanceOf(RuntimeException.class);
    }
  }

  @Test
  @DisplayName("should connect without TLS to an http endpoint")
  @Timeout(30)
  void should_connect_without_tls_to_http_endpoint() {
    var config = ArmoniKConfig.builder()
                              .endpoint("http://localhost:" + plaintextServer.getPort())
                              .build();

    try (var client = new ArmoniKClient(config)) {
      assertThat(client.openSession(sessionDefinition()).sessionInfo().id()).isNotNull();
    }
  }

  private static SessionDefinition sessionDefinition() {
    return new SessionDefinition(Set.of("partition"));
  }
}
