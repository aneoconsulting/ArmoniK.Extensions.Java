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
package fr.aneo.armonik.worker.internal;

import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Utility for parsing network addresses in various formats.
 */
public class AddressResolver {

  private static final Logger logger = LoggerFactory.getLogger(AddressResolver.class);

  private AddressResolver() {
  }

  /**
   * Resolves an address string into an {@link InetSocketAddress}.
   * <p>
   * Returns {@link Optional#empty()} if the address is {@code null} or blank.
   * Throws an exception if the address format is invalid.
   * </p>
   *
   * <h4>Supported Formats</h4>
   * <ul>
   *   <li><strong>IPv4:</strong> {@code host:port} (e.g., {@code 192.168.1.1:8080})</li>
   *   <li><strong>IPv6:</strong> {@code [host]:port} (e.g., {@code [::1]:8080})</li>
   *   <li><strong>Hostname:</strong> {@code host:port} (e.g., {@code localhost:9090})</li>
   *   <li><strong>With protocol:</strong> {@code http://host:port}, {@code tcp://host:port}</li>
   * </ul>
   * <p>
   * Port must be a valid integer between 0 and 65535.
   * Protocol prefixes (http://, https://, tcp://, grpc://) are automatically stripped if present.
   * </p>
   *
   * @param rawAddress the address string; may be {@code null} or blank
   * @return {@link Optional} containing the resolved address, or {@link Optional#empty()} if address is {@code null} or blank
   * @throws IllegalArgumentException if the address format is invalid or port is not numeric
   */
  public static Optional<InetSocketAddress> resolve(String rawAddress) {
    if (rawAddress == null || rawAddress.isBlank()) return Optional.empty();

    if (containsWhitespaceInside(rawAddress)) {
      throw new IllegalArgumentException("Address contains whitespace: " + rawAddress);
    }

    HostPort hostPort = rawAddress.contains("://") ? parseUriStyle(rawAddress) : parseHostAndPort(rawAddress);

    try {
      InetAddress inet = isIpLiteral(hostPort.host)
        ? InetAddresses.forString(hostPort.host)
        : InetAddress.getByName(hostPort.host);
      return Optional.of(new InetSocketAddress(inet, hostPort.port));
    } catch (Exception e) {
      logger.error("Unable to resolve host: {}", rawAddress, e);
      throw new IllegalArgumentException("Unable to resolve host: " + rawAddress, e);
    }
  }

  private static HostPort parseUriStyle(String rawAddress) {
    try {
      URI uri = new URI(rawAddress.trim());
      String host = uri.getHost();
      int port = uri.getPort();

      if (host == null || port == -1) {
        logger.error("URI must include host and port: {}", rawAddress);
        throw new IllegalArgumentException("URI must include host and port: " + rawAddress);
      }
      if ((uri.getPath() != null && !uri.getPath().isEmpty())
        || uri.getQuery() != null
        || uri.getFragment() != null) {
        logger.error("URI must not include path/query/fragment: {}", rawAddress);
        throw new IllegalArgumentException("URI must not include path/query/fragment: " + rawAddress);
      }
      return new HostPort(host, port);
    } catch (URISyntaxException e) {
      logger.error("Invalid URI: {}", rawAddress, e);
      throw new IllegalArgumentException("Invalid URI: " + rawAddress, e);
    }
  }

  private static HostPort parseHostAndPort(String rawAddress) {
    try {
      var hostAndPort = HostAndPort.fromString(rawAddress.trim());
      if (!hostAndPort.hasPort()) {
        logger.error("Port required in address: {}", rawAddress);
        throw new IllegalArgumentException("Port required in address: " + rawAddress);
      }
      if (hostAndPort.getPort() < 0 || hostAndPort.getPort() > 65535) {
        logger.error("Invalid port in address: {}", rawAddress);
        throw new IllegalArgumentException("Port out of range [0–65535]: " + rawAddress);
      }

      return new HostPort(hostAndPort.getHost(), hostAndPort.getPort());
    } catch (IllegalArgumentException ex) {
      logger.error("Invalid host:port address: {}", rawAddress, ex);
      throw new IllegalArgumentException("Invalid host:port address: " + rawAddress, ex);
    }
  }

  private static boolean containsWhitespaceInside(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (Character.isWhitespace(s.charAt(i))) return true;
    }
    return false;
  }

  private static boolean isIpLiteral(String host) {
    try {
      InetAddresses.forString(host);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private record HostPort(String host, int port) {}
}
