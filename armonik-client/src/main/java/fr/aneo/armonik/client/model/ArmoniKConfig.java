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

/**
 * Immutable configuration for connecting to an ArmoniK cluster.
 * <p>
 * This configuration specifies connection parameters including endpoint, security settings,
 * and retry behavior. Create instances using the builder pattern.
 *
 * @see ArmoniKClient
 */
public final class ArmoniKConfig {

  private final String endpoint;
  private final boolean sslValidation;
  private final String caCertPem;
  private final String clientCertPem;
  private final String clientKeyPem;
  private final String clientP12;
  private final String clientP12Password;

  private ArmoniKConfig(Builder builder) {
    this.endpoint = builder.endpoint;
    this.sslValidation = builder.sslValidation;
    this.caCertPem = builder.caCertPem;
    this.clientCertPem = builder.clientCertPem;
    this.clientKeyPem = builder.clientKeyPem;
    this.clientP12 = builder.clientP12;
    this.clientP12Password = builder.clientP12Password;
  }

  /**
   * Returns the ArmoniK cluster endpoint.
   *
   * @return the endpoint URL (e.g., "https://armonik.example.com:443")
   */
  public String endpoint() {
    return endpoint;
  }

  /**
   * Returns whether SSL validation is enabled.
   *
   * @return true if SSL certificates should be validated, false otherwise
   */
  public boolean sslValidation() {
    return sslValidation;
  }

  /**
   * Returns the path to the CA certificate for server verification.
   *
   * @return the CA certificate path, or null to use system trust store
   */
  public String caCertPem() {
    return caCertPem;
  }

  /**
   * Returns the path to the client certificate for mutual TLS (PEM format).
   *
   * @return the client certificate path, or null if not using PEM client auth
   */
  public String clientCertPem() {
    return clientCertPem;
  }

  /**
   * Returns the path to the client private key for mutual TLS (PEM format).
   *
   * @return the client private key path, or null if not using PEM client auth
   */
  public String clientKeyPem() {
    return clientKeyPem;
  }

  /**
   * Returns the path to the PKCS#12 client certificate.
   *
   * @return the PKCS#12 certificate path, or null if not using PKCS#12 client auth
   */
  public String clientP12() {
    return clientP12;
  }

  /**
   * Returns the password for the PKCS#12 client certificate.
   *
   * @return the PKCS#12 password, or null if passwordless
   */
  public String clientP12Password() {
    return clientP12Password;
  }

  /**
   * Creates a new builder for constructing an {@link ArmoniKConfig}.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "ArmoniKConfig{" +
      "endpoint='" + endpoint + '\'' +
      ", sslValidation=" + sslValidation +
      ", caCertPem='" + caCertPem + '\'' +
      ", clientCertPem='" + clientCertPem + '\'' +
      ", clientKeyPem='" + (clientKeyPem != null ? "***" : null) + '\'' +
      ", clientP12='" + clientP12 + '\'' +
      ", clientP12Password='" + (clientP12Password != null ? "***" : null) + '\'' +
      '}';
  }

  /**
   * Builder for {@link ArmoniKConfig}.
   * <p>
   * Use fluent methods to configure the connection parameters, then call {@link #build()}
   * to create an immutable configuration instance.
   */
  public static final class Builder {
    private String endpoint;
    private boolean sslValidation = true;
    private String caCertPem;
    private String clientCertPem;
    private String clientKeyPem;
    private String clientP12;
    private String clientP12Password;

    private Builder() {
    }

    /**
     * Sets the ArmoniK cluster endpoint (required).
     * <p>
     * The endpoint must be a complete URL including protocol and port.
     *
     * @param endpoint the endpoint URL (e.g., "https://armonik.example.com:443")
     * @return this builder
     * @throws NullPointerException if endpoint is null
     */
    public Builder endpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    /**
     * Disables SSL certificate validation.
     * <p>
     * <strong>Security Warning:</strong> This disables certificate validation and should
     * only be used in development environments with self-signed certificates.
     *
     * @return this builder
     */
    public Builder withoutSslValidation() {
      this.sslValidation = false;
      return this;
    }

    /**
     * Sets the CA certificate for server verification.
     * <p>
     * Use this to trust a custom CA instead of the system trust store.
     * If not specified, the system trust store will be used.
     *
     * @param caCertPath path to the CA certificate PEM file
     * @return this builder
     */
    public Builder withCaCert(String caCertPath) {
      this.caCertPem = caCertPath;
      return this;
    }

    /**
     * Sets client certificate for mutual TLS authentication using PEM format.
     * <p>
     * Both certificate and private key files must be provided for PEM-based mTLS.
     *
     * @param certPath path to the client certificate PEM file
     * @param keyPath  path to the client private key PEM file
     * @return this builder
     */
    public Builder withClientCert(String certPath, String keyPath) {
      this.clientCertPem = certPath;
      this.clientKeyPem = keyPath;
      return this;
    }

    /**
     * Sets client certificate for mutual TLS authentication using PKCS#12 format.
     * <p>
     * PKCS#12 files (.p12 or .pfx) bundle the certificate and private key together.
     *
     * @param p12Path  path to the PKCS#12 certificate file
     * @param password password for the PKCS#12 file, or null if passwordless
     * @return this builder
     */
    public Builder withClientP12(String p12Path, String password) {
      this.clientP12 = p12Path;
      this.clientP12Password = password;
      return this;
    }

    /**
     * Sets client certificate for mutual TLS authentication using PKCS#12 format.
     * <p>
     * PKCS#12 files (.p12 or .pfx) bundle the certificate and private key together.
     *
     * @param p12Path  path to the PKCS#12 certificate file
     * @return this builder
     */
    public Builder withClientP12(String p12Path) {
      this.clientP12 = p12Path;
      this.clientP12Password = null;
      return this;
    }

    /**
     * Builds the immutable {@link ArmoniKConfig} instance.
     * <p>
     * Validates that the configuration is consistent and all required fields are set.
     *
     * @return a new {@link ArmoniKConfig} instance
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public ArmoniKConfig build() {
      validate();
      return new ArmoniKConfig(this);
    }

    private void validate() {
      if (endpoint == null || endpoint.isBlank())
        throw new IllegalArgumentException("endpoint is required");

      if (clientCertPem != null && clientKeyPem == null)
        throw new IllegalArgumentException("clientKeyPem is required when clientCertPem is specified");

      if (clientKeyPem != null && clientCertPem == null)
        throw new IllegalArgumentException("clientCertPem is required when clientKeyPem is specified");

      if (clientP12 != null && clientCertPem != null)
        throw new IllegalArgumentException("Cannot specify both PKCS#12 (clientP12) and PEM (clientCertPem/clientKeyPem) certificates. Choose one certificate format.");
    }
  }
}
