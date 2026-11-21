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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.*;

class ArmoniKConfigTest {

  @Test
  @DisplayName("should build config with only endpoint")
  void should_build_config_with_only_endpoint() {
    // When
    var config = ArmoniKConfig.builder()
                              .endpoint("https://armonik.example.com:443")
                              .build();

    // Then
    assertThat(config.endpoint()).isEqualTo("https://armonik.example.com:443");
    assertThat(config.sslValidation()).isTrue();
    assertThat(config.caCertPem()).isNull();
    assertThat(config.clientCertPem()).isNull();
    assertThat(config.clientKeyPem()).isNull();
    assertThat(config.clientP12()).isNull();
    assertThat(config.clientP12Password()).isNull();
  }

  @Test
  @DisplayName("should build config with CA certificate")
  void should_build_config_with_ca_certificate() {
    // When
    var config = ArmoniKConfig.builder()
                              .endpoint("https://armonik.example.com:443")
                              .withCaCert("/path/to/ca.pem")
                              .build();

    // Then
    assertThat(config.endpoint()).isEqualTo("https://armonik.example.com:443");
    assertThat(config.sslValidation()).isTrue();
    assertThat(config.caCertPem()).isEqualTo("/path/to/ca.pem");
  }

  @Test
  @DisplayName("should build config with PEM client certificate")
  void should_build_config_with_pem_client_certificate() {
    // When
    var config = ArmoniKConfig.builder()
                              .endpoint("https://armonik.example.com:443")
                              .withClientCert("/path/to/client.pem", "/path/to/client.key")
                              .build();

    // Then
    assertThat(config.endpoint()).isEqualTo("https://armonik.example.com:443");
    assertThat(config.clientCertPem()).isEqualTo("/path/to/client.pem");
    assertThat(config.clientKeyPem()).isEqualTo("/path/to/client.key");
  }

  @Test
  @DisplayName("should build config with PKCS12 client certificate with password")
  void should_build_config_with_pkcs12_client_certificate_with_password() {
    // When
    var config = ArmoniKConfig.builder()
                              .endpoint("https://armonik.example.com:443")
                              .withClientP12("/path/to/client.p12", "password")
                              .build();

    // Then
    assertThat(config.endpoint()).isEqualTo("https://armonik.example.com:443");
    assertThat(config.clientP12()).isEqualTo("/path/to/client.p12");
    assertThat(config.clientP12Password()).isEqualTo("password");
  }

  @Test
  @DisplayName("should build config with passwordless PKCS12 certificate using single argument method")
  void should_build_config_with_passwordless_pkcs12_certificate_using_single_argument_method() {
    // When
    var config = ArmoniKConfig.builder()
                              .endpoint("https://armonik.example.com:443")
                              .withClientP12("/path/to/client.p12")
                              .build();

    // Then
    assertThat(config.clientP12()).isEqualTo("/path/to/client.p12");
    assertThat(config.clientP12Password()).isNull();
  }

  @Test
  @DisplayName("should build config without SSL validation for an https endpoint")
  void should_build_config_without_ssl_validation() {
    // When
    var config = ArmoniKConfig.builder()
                              .endpoint("https://localhost:4312")
                              .withoutSslValidation()
                              .build();

    // Then
    assertThat(config.endpoint()).isEqualTo("https://localhost:4312");
    assertThat(config.sslValidation()).isFalse();
    assertThat(config.usesTls()).isTrue();
  }

  @Test
  @DisplayName("should connect without TLS to an http endpoint and over TLS to an https one")
  void should_choose_the_transport_from_the_scheme() {
    assertThat(ArmoniKConfig.builder().endpoint("http://localhost:4312").build().usesTls()).isFalse();
    assertThat(ArmoniKConfig.builder().endpoint("HTTPS://armonik.example.com:443").build().usesTls()).isTrue();
  }

  @Test
  @DisplayName("should reject an endpoint whose scheme is neither http nor https")
  void should_reject_endpoint_without_http_scheme() {
    assertThatThrownBy(() -> ArmoniKConfig.builder().endpoint("localhost:4312").build())
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("localhost:4312");
    assertThatThrownBy(() -> ArmoniKConfig.builder().endpoint("grpc://localhost:4312").build())
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should reject withoutSslValidation with an http endpoint, which has no TLS")
  void should_reject_without_ssl_validation_on_http_endpoint() {
    assertThatThrownBy(() -> ArmoniKConfig.builder().endpoint("http://localhost:4312").withoutSslValidation().build())
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("https://");
  }

  @Test
  @DisplayName("should reject certificates with an http endpoint, which has no TLS")
  void should_reject_certificates_on_http_endpoint() {
    var builder = ArmoniKConfig.builder().endpoint("http://localhost:4312");

    assertThatThrownBy(() -> builder.withCaCert("/path/to/ca.pem").build()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ArmoniKConfig.builder().endpoint("http://localhost:4312").withClientCert("/cert.pem", "/key.pem").build())
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ArmoniKConfig.builder().endpoint("http://localhost:4312").withClientP12("/client.p12", "secret").build())
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should reject a CA certificate with withoutSslValidation, since it would not be used")
  void should_reject_ca_cert_without_ssl_validation() {
    assertThatThrownBy(() -> ArmoniKConfig.builder().endpoint("https://localhost:4312").withCaCert("/path/to/ca.pem").withoutSslValidation().build())
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("/path/to/ca.pem");
  }

  @Test
  @DisplayName("should build complete config with all options")
  void should_build_complete_config_with_all_options() {
    // When
    var retryPolicy = new RetryPolicy(10, Duration.ofSeconds(5), Duration.ofSeconds(10), 2.0);
    var config = ArmoniKConfig.builder()
                              .endpoint("https://armonik.example.com:443")
                              .withCaCert("/path/to/ca.pem")
                              .withClientCert("/path/to/client.pem", "/path/to/client.key")
                              .withRetryPolicy(retryPolicy)
                              .build();

    // Then
    assertThat(config.endpoint()).isEqualTo("https://armonik.example.com:443");
    assertThat(config.sslValidation()).isTrue();
    assertThat(config.caCertPem()).isEqualTo("/path/to/ca.pem");
    assertThat(config.clientCertPem()).isEqualTo("/path/to/client.pem");
    assertThat(config.clientKeyPem()).isEqualTo("/path/to/client.key");
    assertThat(config.retryPolicy()).isEqualTo(retryPolicy);
  }

  @Test
  @DisplayName("should throw exception when endpoint is null")
  void should_throw_exception_when_endpoint_is_null() {
    // When/Then
    assertThatThrownBy(() -> ArmoniKConfig.builder().build())
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when endpoint is blank")
  void should_throw_exception_when_endpoint_is_blank() {
    // When/Then
    assertThatThrownBy(() -> ArmoniKConfig.builder()
                                          .endpoint("   ")
                                          .build())
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when client cert is provided without key")
  void should_throw_exception_when_client_cert_is_provided_without_key() {
    // When/Then
    assertThatThrownBy(() -> ArmoniKConfig.builder()
                                          .endpoint("https://armonik.example.com:443")
                                          .withClientCert("/path/to/client.pem", null)
                                          .build())
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when client key is provided without cert")
  void should_throw_exception_when_client_key_is_provided_without_cert() {
    // When/Then
    assertThatThrownBy(() -> ArmoniKConfig.builder()
                                          .endpoint("https://armonik.example.com:443")
                                          .withClientCert(null, "/path/to/client.key")
                                          .build())
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when both PEM and PKCS12 certificates are provided")
  void should_throw_exception_when_both_pem_and_pkcs12_certificates_are_provided() {
    // When/Then
    assertThatThrownBy(() -> ArmoniKConfig.builder()
                                          .endpoint("https://armonik.example.com:443")
                                          .withClientCert("/path/to/client.pem", "/path/to/client.key")
                                          .withClientP12("/path/to/client.p12", "password")
                                          .build())
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should use default task submission batching unless configured")
  void should_configure_task_submission_batching() {
    var defaultConfig = ArmoniKConfig.builder().endpoint("http://localhost:5001").build();
    var customBatching = new TaskSubmissionBatching(10, Duration.ofMillis(5));
    var customConfig = ArmoniKConfig.builder().endpoint("http://localhost:5001").withTaskSubmissionBatching(customBatching).build();

    assertThat(defaultConfig.taskSubmissionBatching()).isEqualTo(TaskSubmissionBatching.DEFAULT);
    assertThat(customConfig.taskSubmissionBatching()).isEqualTo(customBatching);
  }

  @Test
  @DisplayName("should default the channel pool size to 50 and accept a custom one")
  void should_default_and_customize_channel_pool_size() {
    var builder = ArmoniKConfig.builder().endpoint("https://armonik.example.com:443");

    assertThat(builder.build().channelPoolSize()).isEqualTo(ArmoniKConfig.DEFAULT_CHANNEL_POOL_SIZE).isEqualTo(50);
    assertThat(builder.withChannelPoolSize(1).build().channelPoolSize()).isEqualTo(1);
    assertThat(builder.withChannelPoolSize(200).build().channelPoolSize()).isEqualTo(200);
  }

  @Test
  @DisplayName("should reject a channel pool size below 1")
  void should_reject_channel_pool_size_below_one() {
    var builder = ArmoniKConfig.builder().endpoint("https://armonik.example.com:443");

    assertThatThrownBy(() -> builder.withChannelPoolSize(0).build()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.withChannelPoolSize(-1).build()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should default the event channel pool size to 50, accept a custom one and reject one below 1")
  void should_default_customize_and_validate_event_channel_pool_size() {
    var builder = ArmoniKConfig.builder().endpoint("https://armonik.example.com:443");

    assertThat(builder.build().eventChannelPoolSize()).isEqualTo(ArmoniKConfig.DEFAULT_EVENT_CHANNEL_POOL_SIZE).isEqualTo(50);
    assertThat(builder.withEventChannelPoolSize(8).build().eventChannelPoolSize()).isEqualTo(8);
    assertThatThrownBy(() -> builder.withEventChannelPoolSize(0).build()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should keep the listener executor supplied by the application")
  void should_keep_the_listener_executor() {
    Executor executor = Runnable::run;
    var builder = ArmoniKConfig.builder().endpoint("https://armonik.example.com:443");

    assertThat(builder.build().listenerExecutor()).isNull();
    assertThat(builder.withListenerExecutor(executor).build().listenerExecutor()).isSameAs(executor);
  }

  @Test
  @DisplayName("should default the download budget to 512 MiB, half of it per session, and validate it")
  void should_configure_download_budget() {
    var builder = ArmoniKConfig.builder().endpoint("https://armonik.example.com:443");

    var defaults = builder.build();
    assertThat(defaults.downloadBudget()).isEqualTo(ArmoniKConfig.DEFAULT_DOWNLOAD_BUDGET).isEqualTo(512L * 1024 * 1024);
    assertThat(defaults.sessionDownloadBudget()).isEqualTo(256L * 1024 * 1024);

    var custom = builder.withDownloadBudget(1000, 300).build();
    assertThat(custom.downloadBudget()).isEqualTo(1000);
    assertThat(custom.sessionDownloadBudget()).isEqualTo(300);

    assertThat(builder.withDownloadBudget(1000, 5000).build().sessionDownloadBudget()).isEqualTo(1000);
    assertThat(builder.withDownloadBudget(1000).build().sessionDownloadBudget()).isEqualTo(500);
    assertThat(builder.withDownloadBudget(Long.MAX_VALUE).build().downloadBudget()).isEqualTo(Long.MAX_VALUE);
    assertThat(builder.withDownloadBudget(Long.MAX_VALUE).build().sessionDownloadBudget()).isEqualTo(Long.MAX_VALUE);
    assertThat(builder.withDownloadBudget(Long.MAX_VALUE, 1000).build().sessionDownloadBudget()).isEqualTo(1000);

    assertThatThrownBy(() -> builder.withDownloadBudget(0).build()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.withDownloadBudget(1000, -1).build()).isInstanceOf(IllegalArgumentException.class);
  }
}
