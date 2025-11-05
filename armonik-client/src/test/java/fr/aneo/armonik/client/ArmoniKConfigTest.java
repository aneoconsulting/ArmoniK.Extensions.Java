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
  @DisplayName("should build config without SSL validation")
  void should_build_config_without_ssl_validation() {
    // When
    var config = ArmoniKConfig.builder()
                              .endpoint("http://localhost:4312")
                              .withoutSslValidation()
                              .build();

    // Then
    assertThat(config.endpoint()).isEqualTo("http://localhost:4312");
    assertThat(config.sslValidation()).isFalse();
  }

  @Test
  @DisplayName("should build complete config with all options")
  void should_build_complete_config_with_all_options() {
    // When
    var config = ArmoniKConfig.builder()
                              .endpoint("https://armonik.example.com:443")
                              .withCaCert("/path/to/ca.pem")
                              .withClientCert("/path/to/client.pem", "/path/to/client.key")
                              .build();

    // Then
    assertThat(config.endpoint()).isEqualTo("https://armonik.example.com:443");
    assertThat(config.sslValidation()).isTrue();
    assertThat(config.caCertPem()).isEqualTo("/path/to/ca.pem");
    assertThat(config.clientCertPem()).isEqualTo("/path/to/client.pem");
    assertThat(config.clientKeyPem()).isEqualTo("/path/to/client.key");
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
}
