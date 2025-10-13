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


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static uk.org.webcompere.systemstubs.SystemStubs.withEnvironmentVariables;

class ArmoniKWorkerTest {

  private ArmoniKWorker armoniKWorker;

  @BeforeEach
  void setUp() {
    armoniKWorker = new ArmoniKWorker(mock(TaskProcessor.class));
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    armoniKWorker.shutdown();
  }

  @Test
  @DisplayName("should fail to start when ComputePlane__AgentChannel__Address is missing")
  void should_fail_when_agent_channel_env_missing() throws Exception {
    withEnvironmentVariables()
      .execute(() -> {
        assertThatThrownBy(() -> armoniKWorker.start())
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessageContaining("ComputePlane__AgentChannel__Address is not set");
      });
  }

  @Test
  @DisplayName("should use default worker address when environment variable is missing")
  void should_use_default_worker_address_when_environment_variable_is_missing() throws Exception {
    // Given
    withEnvironmentVariables("ComputePlane__AgentChannel__Address", "127.0.0.1:50052")
      .execute(() -> {
        // When
        armoniKWorker.start();
        // Then
        assertThat(armoniKWorker.address())
          .extracting(InetSocketAddress::getHostString, InetSocketAddress::getPort)
          .containsExactly("0.0.0.0", 8080);
      });
  }

  @Test
  @DisplayName("should start the server with IP and port from environment variable")
  void should_parse_valid_ip_port() throws Exception {
    // Given
    withEnvironmentVariables(
      "ComputePlane__AgentChannel__Address", "127.0.0.1:50052",
      "ComputePlane__WorkerChannel__Address", "127.0.0.1:50051")
      .execute(() -> {
        // When
        armoniKWorker.start();

        // Then
        assertThat(armoniKWorker.address())
          .extracting(InetSocketAddress::getHostString, InetSocketAddress::getPort)
          .containsExactly("127.0.0.1", 50051);
      });
  }
}
