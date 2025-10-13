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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Optional;

/**
 * Utility for parsing network addresses in {@code host:port} format.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Valid addresses
 * Optional<InetSocketAddress> addr1 = AddressResolver.resolve("192.168.1.100:8080");
 * Optional<InetSocketAddress> addr2 = AddressResolver.resolve("localhost:9090");
 *
 * // Missing address returns empty
 * Optional<InetSocketAddress> empty = AddressResolver.resolve(null);
 *
 * // Invalid format throws exception
 * AddressResolver.resolve("192.168.1.1");           // missing port
 * AddressResolver.resolve("host:abc");              // invalid port
 * }</pre>
 *
 * @see fr.aneo.armonik.worker.ArmoniKWorker
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
   * <h4>Format</h4>
   * <p>Address must be: {@code host:port}</p>
   * <ul>
   *   <li><strong>host</strong>: IP address or hostname</li>
   *   <li><strong>port</strong>: Integer port number</li>
   * </ul>
   *
   * @param address the address string in {@code host:port} format; may be {@code null} or blank
   * @return {@link Optional} containing the resolved address, or {@link Optional#empty()} if address is {@code null} or blank
   * @throws IllegalArgumentException if the address format is invalid or port is not numeric
   */
  public static Optional<InetSocketAddress> resolve(String address) {
    if (address == null || address.isBlank()) return Optional.empty();

    String[] parts = address.split(":");
    if (parts.length != 2) {
      logger.error("Invalid address {}", address);
      throw new IllegalArgumentException("Address must be in the form 'ip:port'");
    }

    String host = parts[0];
    int port;
    try {
      port = Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      logger.error("Invalid port {}", address);
      throw new IllegalArgumentException("Invalid port: " + parts[1], e);
    }

    var inetSocketAddress = new InetSocketAddress(host, port);
    return Optional.of(inetSocketAddress);
  }
}
