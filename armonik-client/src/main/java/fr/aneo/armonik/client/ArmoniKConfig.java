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

import java.net.URI;

import static fr.aneo.armonik.client.RetryPolicy.DEFAULT;
import static java.util.Objects.requireNonNullElse;

/**
 * Immutable configuration for connecting to an ArmoniK cluster.
 * <p>
 * This configuration specifies connection parameters including endpoint, security settings,
 * and retry behavior. Create instances using the builder pattern.
 * <p>
 * The scheme of the endpoint chooses the transport: {@code https://} connects over TLS and validates
 * the server certificate, unless {@link Builder#withoutSslValidation()} is set; {@code http://}
 * connects without TLS.
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
  private final int eventChannelPoolSize;

  /**
   * Default number of channels, that is of calls in flight, per client.
   */
  public static final int DEFAULT_CHANNEL_POOL_SIZE = 50;

  /**
   * Default number of event channels, that is of output event streams open at once, per client.
   */
  public static final int DEFAULT_EVENT_CHANNEL_POOL_SIZE = 50;

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
    this.eventChannelPoolSize = builder.eventChannelPoolSize;
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
   * Returns whether the server certificate of an {@code https://} endpoint is validated.
   *
   * @return true if the server certificate is validated, false if any certificate is accepted
   * @see Builder#withoutSslValidation()
   */
  public boolean sslValidation() {
    return sslValidation;
  }

  /**
   * Returns whether the endpoint connects over TLS, that is whether its scheme is {@code https}.
   */
  boolean usesTls() {
    return usesTls(endpoint);
  }

  /**
   * Returns whether {@code endpoint} connects over TLS: {@code https} does, {@code http} does not.
   *
   * @throws IllegalArgumentException if the endpoint is not a URL or its scheme is neither
   */
  private static boolean usesTls(String endpoint) {
    String scheme;
    try {
      scheme = URI.create(endpoint).getScheme();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("endpoint is not a valid URL: " + endpoint, e);
    }
    if ("https".equalsIgnoreCase(scheme)) return true;
    if ("http".equalsIgnoreCase(scheme)) return false;
    throw new IllegalArgumentException("endpoint must start with http:// or https://, got: " + endpoint);
  }

  private static boolean isSet(String value) {
    return value != null && !value.isBlank();
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
   * Returns the number of event channels of the client, which is also the number of output event
   * streams it keeps open at once.
   *
   * @return the event channel pool size, at least 1
   * @see Builder#withEventChannelPoolSize(int)
   */
  public int eventChannelPoolSize() {
    return eventChannelPoolSize;
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
    private int eventChannelPoolSize = DEFAULT_EVENT_CHANNEL_POOL_SIZE;
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
     * The endpoint must be a complete URL including scheme and port. Its scheme chooses the
     * transport: {@code https://} connects over TLS, {@code http://} connects without TLS and
     * accepts no certificate setting.
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
     * Disables the validation of the server certificate, while keeping TLS.
     * <p>
     * The connection to an {@code https://} endpoint stays encrypted, but any server certificate is
     * accepted, without validating the chain that issued it nor the host name it was issued for.
     * A client certificate is still sent for mutual TLS. To connect without TLS, use an
     * {@code http://} endpoint instead.
     * <p>
     * <strong>Security Warning:</strong> the server is not authenticated: anyone able to intercept
     * the connection can impersonate it. Use this only in development, for instance against a
     * control plane with a self-signed certificate.
     * <p>
     * {@link #build()} rejects it with an {@code http://} endpoint, which has no certificate to
     * validate, and with {@link #withCaCert(String)}, whose CA would not be used.
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
     * Sets the number of event channels of the client, kept apart from its other channels.
     * <p>
     * Outputs are watched through {@code GetEvents} streams, one per session for up to 1000 pending
     * outputs. Each stream holds one event channel, that is one HTTP/2 connection, for as long as
     * its session waits for outputs, so that waiting for outputs never takes a channel from the other
     * calls. Beyond this number of streams, further streams wait for one to close, and their outputs
     * are reported only then. Since the stream of a session whose outputs do not progress, a paused
     * session for instance, never closes, that wait can last indefinitely: a warning is logged when a
     * stream has waited 5 s for a channel. Size the pool for the number of sessions that wait for
     * outputs at the same time.
     *
     * @param eventChannelPoolSize the number of event channels, at least 1; defaults to {@link ArmoniKConfig#DEFAULT_EVENT_CHANNEL_POOL_SIZE}
     * @return this builder
     */
    public Builder withEventChannelPoolSize(int eventChannelPoolSize) {
      this.eventChannelPoolSize = eventChannelPoolSize;
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

      var plaintext = !usesTls(endpoint);
      if (plaintext && !sslValidation)
        throw new IllegalArgumentException("withoutSslValidation() cannot be used with an http:// endpoint, which connects without TLS: " +
          "use an https:// endpoint to keep TLS without validating the server certificate, or remove withoutSslValidation(). Endpoint: " + endpoint);

      if (plaintext && (isSet(caCertPem) || clientCertPem != null || clientKeyPem != null || clientP12 != null))
        throw new IllegalArgumentException("Certificates cannot be used with an http:// endpoint, which connects without TLS: " +
          "use an https:// endpoint. Endpoint: " + endpoint);

      if (!sslValidation && isSet(caCertPem))
        throw new IllegalArgumentException("withCaCert(" + caCertPem + ") cannot be used with withoutSslValidation(), " +
          "since the CA certificate would not be used: remove one of them");

      if (channelPoolSize < 1)
        throw new IllegalArgumentException("channelPoolSize must be at least 1, got: " + channelPoolSize);

      if (eventChannelPoolSize < 1)
        throw new IllegalArgumentException("eventChannelPoolSize must be at least 1, got: " + eventChannelPoolSize);

      if (clientCertPem != null && clientKeyPem == null)
        throw new IllegalArgumentException("clientKeyPem is required when clientCertPem is specified");

      if (clientKeyPem != null && clientCertPem == null)
        throw new IllegalArgumentException("clientCertPem is required when clientKeyPem is specified");

      if (clientP12 != null && clientCertPem != null)
        throw new IllegalArgumentException("Cannot specify both PKCS#12 (clientP12) and PEM (clientCertPem/clientKeyPem) certificates. Choose one certificate format.");
    }
  }
}
