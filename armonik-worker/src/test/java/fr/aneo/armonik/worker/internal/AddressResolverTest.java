package fr.aneo.armonik.worker.internal;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AddressResolverTest {

  @Test
  void should_parse_valid_ip_port() {
    // When
    var address = AddressResolver.resolve("127.0.0.1:50051");

    // Then
    assertThat(address).isNotEmpty()
                       .get()
                       .extracting(InetSocketAddress::getHostString, InetSocketAddress::getPort)
                       .containsExactly("127.0.0.1", 50051);
  }

  @Test
  void should_throw_exception_when_address_is_unparseable() {
    assertThatThrownBy(() -> AddressResolver.resolve("unparseable"))
      .isInstanceOf(IllegalArgumentException.class);
  }


  @Test
  void should_throw_exception_when_port_is_not_numeric() {
    assertThatThrownBy(() -> AddressResolver.resolve("127.0.0.1:http"))
      .isInstanceOf(IllegalArgumentException.class);
  }
}
