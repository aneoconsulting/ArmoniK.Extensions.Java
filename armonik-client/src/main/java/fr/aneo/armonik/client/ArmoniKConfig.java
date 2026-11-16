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

import java.util.concurrent.Executor;

import static fr.aneo.armonik.client.RetryPolicy.DEFAULT;
import static java.util.Objects.requireNonNullElse;

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
  private final RetryPolicy retryPolicy;
  private final TaskSubmissionBatching taskSubmissionBatching;
  private final int channelPoolSize;

  /**
   * Default number of channels, that is of calls in flight, per client.
   */
  public static final int DEFAULT_CHANNEL_POOL_SIZE = 50;

  private final Executor listenerExecutor;

  private ArmoniKConfig(Builder builder) {
    this.endpoint = builder.endpoint;
    this.sslValidation = builder.sslValidation;
    this.caCertPem = builder.caCertPem;
    this.clientCertPem = builder.clientCertPem;
    this.clientKeyPem = builder.clientKeyPem;
    this.clientP12 = builder.clientP12;
    this.clientP12Password = builder.clientP12Password;
    this.retryPolicy = builder.retryPolicy;
    this.taskSubmissionBatching = builder.taskSubmissionBatching;
    this.channelPoolSize = builder.channelPoolSize;
    this.listenerExecutor = builder.listenerExecutor;
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
   * Returns the retry policy for gRPC operations.
   * <p>
   * The retry policy controls automatic retry behavior for both unary calls
   * (at the channel level) and streaming operations (at the application level).
   *
   * @return the retry policy configuration
   * @see RetryPolicy
   */
  public RetryPolicy retryPolicy() {
    return retryPolicy;
  }

  /**
   * Returns how task submissions are grouped into batched calls.
   *
   * @return the task submission batching settings; {@link TaskSubmissionBatching#DEFAULT} unless configured
   * @see Builder#withTaskSubmissionBatching(TaskSubmissionBatching)
   */
  public TaskSubmissionBatching taskSubmissionBatching() {
    return taskSubmissionBatching;
  }

  /**
   * Returns the number of channels of the client, which is also the number of calls it runs at once.
   *
   * @return the channel pool size, at least 1
   * @see Builder#withChannelPoolSize(int)
   */
  public int channelPoolSize() {
    return channelPoolSize;
  }

  /**
   * Returns the executor running the {@link BlobCompletionListener} calls, or null when the client
   * creates its own.
   *
   * @return the application-supplied listener executor, or null
   */
  public Executor listenerExecutor() {
    return listenerExecutor;
  }

  /**
   * Creates a new builder for constructing an {@link ArmoniKConfig}.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link ArmoniKConfig}.
   * <p>
   * Use fluent methods to configure the connection parameters, then call {@link #build()}
   * to create an immutable configuration instance.
   */
  public static final class Builder {
    private RetryPolicy retryPolicy = DEFAULT;
    private TaskSubmissionBatching taskSubmissionBatching = TaskSubmissionBatching.DEFAULT;
    private int channelPoolSize = DEFAULT_CHANNEL_POOL_SIZE;
    private Executor listenerExecutor;
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
     * Sets the retry policy for gRPC operations.
     * <p>
     * The retry policy controls automatic retry behavior for transient failures.
     * If not specified, {@link RetryPolicy#DEFAULT} is used.
     *
     * @param retryPolicy the retry policy to use, or null to use defaults
     * @return this builder
     * @see RetryPolicy
     */
    public Builder withRetryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = requireNonNullElse(retryPolicy, RetryPolicy.DEFAULT);
      return this;
    }

    /**
     * Sets how task submissions are grouped into batched calls.
     *
     * @param taskSubmissionBatching the batching settings; {@code null} restores {@link TaskSubmissionBatching#DEFAULT}
     * @return this builder
     * @see TaskSubmissionBatching
     */
    public Builder withTaskSubmissionBatching(TaskSubmissionBatching taskSubmissionBatching) {
      this.taskSubmissionBatching = requireNonNullElse(taskSubmissionBatching, TaskSubmissionBatching.DEFAULT);
      return this;
    }

    /**
     * Sets the number of channels of the client, that is of HTTP/2 connections to the control plane.
     * <p>
     * Each channel carries one call at a time, so this is also the number of calls the client runs at
     * once, whichever session starts them; a call that finds every channel busy waits for one without
     * blocking any thread. One connection per call costs one TCP and TLS connection per call in flight,
     * and caps the throughput at about this size divided by the duration of a call, network round
     * trip included. Raise it for a distant control plane or many concurrent transfers.
     *
     * @param channelPoolSize the number of channels, at least 1; defaults to {@link ArmoniKConfig#DEFAULT_CHANNEL_POOL_SIZE}
     * @return this builder
     */
    public Builder withChannelPoolSize(int channelPoolSize) {
      this.channelPoolSize = channelPoolSize;
      return this;
    }

    /**
     * Sets the executor running the {@link BlobCompletionListener} calls.
     * <p>
     * Listener calls are application code and may take time: they never run on the threads that
     * complete the client's gRPC calls, so a slow listener only delays other listener calls, never
     * submissions, uploads or downloads. By default each client runs them on its own pool of one
     * daemon thread per available processor, shut down with the client. An executor supplied here
     * is used as is, and left running when the client closes.
     *
     * @param listenerExecutor the executor, or null for the client's own
     * @return this builder
     */
    public Builder withListenerExecutor(Executor listenerExecutor) {
      this.listenerExecutor = listenerExecutor;
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

      if (channelPoolSize < 1)
        throw new IllegalArgumentException("channelPoolSize must be at least 1, got: " + channelPoolSize);

      if (clientCertPem != null && clientKeyPem == null)
        throw new IllegalArgumentException("clientKeyPem is required when clientCertPem is specified");

      if (clientKeyPem != null && clientCertPem == null)
        throw new IllegalArgumentException("clientCertPem is required when clientKeyPem is specified");

      if (clientP12 != null && clientCertPem != null)
        throw new IllegalArgumentException("Cannot specify both PKCS#12 (clientP12) and PEM (clientCertPem/clientKeyPem) certificates. Choose one certificate format.");
    }
  }
}
