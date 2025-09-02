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

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Connection configuration for the ArmoniK client.
 * <p>
 * This record aggregates endpoint, TLS credentials, and retry backoff parameters used to establish
 * connections to the ArmoniK services.
 * </p>
 *
 * <p>Validation rules:</p>
 * <ul>
 *   <li>{@code retryInitialBackoff} and {@code retryMaxBackoff} must be positive.</li>
 *   <li>{@code retryBackoffMultiplier} must be greater than 1.0.</li>
 *   <li>{@code retryMaxBackoff} must be greater than or equal to {@code retryInitialBackoff}.</li>
 * </ul>
 *
 * <p>Defaults:</p>
 * <ul>
 *   <li>When {@code retryInitialBackoff} is {@code null}, a default of 1s is applied.</li>
 *   <li>When {@code retryMaxBackoff} is {@code null}, a default of 30s is applied.</li>
 * </ul>
 *
 * @param endpoint               endpoint URL (e.g., {@code http://host:5001} or {@code https://host:5001})
 * @param sslValidation          whether TLS certificates are strictly validated
 * @param caCertPem              optional path to a CA certificate (PEM)
 * @param clientCertPem          optional path to the client certificate (PEM)
 * @param clientKeyPem           optional path to the client private key (PEM)
 * @param clientP12              optional path to a client P12 bundle
 * @param targetNameOverride     optional TLS authority override (testing)
 * @param retryInitialBackoff    initial backoff duration for retries (defaults to 1s when {@code null})
 * @param retryBackoffMultiplier multiplicative factor applied to the backoff for successive retries (must be > 1.0)
 * @param retryMaxBackoff        maximum backoff duration for retries (defaults to 30s when {@code null})
 */

public record ArmoniKConnectionConfig(
  String endpoint,
  boolean sslValidation,
  String caCertPem,
  String clientCertPem,
  String clientKeyPem,
  String clientP12,
  String targetNameOverride,
  Duration retryInitialBackoff,
  double retryBackoffMultiplier,
  Duration retryMaxBackoff //
) {

  /**
   * Applies default values and validates retry parameters.
   *
   * @throws IllegalArgumentException if retry parameters are invalid
   */
  public ArmoniKConnectionConfig {
    retryInitialBackoff = retryInitialBackoff == null ? Duration.of(1, SECONDS) : retryInitialBackoff;
    retryMaxBackoff = retryMaxBackoff == null ? Duration.of(30, SECONDS) : retryMaxBackoff;

    if (retryInitialBackoff.isNegative()) {
      throw new IllegalArgumentException("retryInitialBackoff must be positive");
    }

    if (retryBackoffMultiplier <= 1.0) {
      throw new IllegalArgumentException("retryBackoffMultiplier must be greater than 1.0");
    }
    if (retryMaxBackoff.isNegative()) {
      throw new IllegalArgumentException("retryMaxBackoff must be positive");
    }

    if (retryMaxBackoff.compareTo(retryInitialBackoff) < 0) {
      throw new IllegalArgumentException("retryMaxBackoff must be greater than retryInitialBackoff");
    }
  }

  /**
   * Convenience factory for an unsecured connection configuration bound to the given endpoint.
   * <p>
   * Disables TLS validation and applies default retry settings.
   * </p>
   *
   * @param endpoint the service endpoint URL
   * @return a configuration with TLS validation disabled and default retry parameters
   */
  public static ArmoniKConnectionConfig unsecureForEndpoint(String endpoint) {
    return new ArmoniKConnectionConfig(
      endpoint,
      false,
      null,
      null,
      null,
      null,
      null,
      Duration.of(1, SECONDS),
      2,
      Duration.of(30, SECONDS));
  }
}
