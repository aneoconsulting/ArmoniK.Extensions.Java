package fr.aneo.armonik.worker.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Optional;

public class AddressResolver {

  private static final Logger logger = LoggerFactory.getLogger(AddressResolver.class);

  private AddressResolver() {
  }

  public static Optional<InetSocketAddress> resolve(String address) {
    if (address == null || address.isBlank()) return Optional.empty();

    var parts = address.split(":");
    if (parts.length != 2)
      throw new IllegalArgumentException("Invalid worker address format. Expected 'ip:port' but got: " + address);

    String host = parts[0].trim();
    String portAsString = parts[1].trim();

    validateHost(host, address);
    int port = parseAndValidatePort(portAsString, address);

    logger.info("Configured worker address from environment: {}:{}", host, port);
    return Optional.of(new InetSocketAddress(host, port));
  }

  private static void validateHost(String host, String rawAddress) {
    String[] octets = host.split("\\.");
    if (octets.length != 4)
      throw new IllegalArgumentException("Invalid IPv4 '" + host + "' in worker address: " + rawAddress);

    Arrays.stream(octets).forEach(octet -> {
      if (!octet.chars().allMatch(Character::isDigit))
        throw new IllegalArgumentException("Invalid IPv4 '" + host + "' in worker address: " + rawAddress);
      int value = Integer.parseInt(octet);
      if (value < 0 || value > 255)
        throw new IllegalArgumentException("Invalid IPv4 '" + host + "' in worker address: " + rawAddress);
    });
  }

  private static int parseAndValidatePort(String portStr, String rawAddress) {
    try {
      int port = Integer.parseInt(portStr);
      if (port < 1 || port > 65535)
        throw new IllegalArgumentException("Invalid port '" + port + "' in worker address: " + rawAddress +
          ". Port must be between 1 and 65535.");
      return port;
    } catch (NumberFormatException e) {
      logger.error("Invalid port '{}' in worker address '{}'. Must be numeric.", portStr, rawAddress);
      throw new IllegalArgumentException("Invalid port '" + portStr + "' in worker address: " + rawAddress +
        ". Port must be numeric.", e);
    }
  }
}
