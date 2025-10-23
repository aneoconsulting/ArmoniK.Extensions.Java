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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static com.google.common.net.InetAddresses.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AddressResolverTest {

  @Test
  @DisplayName("should parse valid ip port")
  void should_parse_valid_ip_port() {
    // When
    var address = AddressResolver.resolve("182.170.0.1:50051");

    // Then
    assertThat(address).isNotEmpty()
                       .get()
                       .extracting(InetSocketAddress::getHostString, InetSocketAddress::getPort)
                       .containsExactly("182.170.0.1", 50051);
  }

  @Test
  @DisplayName("should parse http protocol with localhost")
  void should_parse_with_http_protocol() {
    // When
    var address = AddressResolver.resolve("http://localhost:10667");

    // Then
    assertThat(address).isNotEmpty()
                       .get()
                       .extracting(InetSocketAddress::getHostName, InetSocketAddress::getPort)
                       .containsExactly("localhost", 10667);
  }

  @Test
  @DisplayName("should parse ipv6 with brackets")
  void should_parse_ipv6_with_brackets() {
    // When
    var address = AddressResolver.resolve("[::1]:8080");

    // Then
    assertThat(address).isNotEmpty()
                       .get()
                       .extracting(a -> toAddrString(a.getAddress()), InetSocketAddress::getPort)
                       .containsExactly("::1", 8080);
  }

  @Test
  @DisplayName("should parse ipv6 full address with brackets")
  void should_parse_ipv6_full_address() {
    // When
    var address = AddressResolver.resolve("[2001:db8::1]:9090");

    // Then
    assertThat(address).isNotEmpty()
                       .get()
                       .extracting(a -> toAddrString(a.getAddress()), InetSocketAddress::getPort)
                       .containsExactly("2001:db8::1", 9090);
  }


  @Test
  @DisplayName("should return empty when address is null")
  void should_return_empty_when_address_is_null() {
    // When
    var address = AddressResolver.resolve(null);

    // Then
    assertThat(address).isEmpty();
  }

  @Test
  @DisplayName("should return empty when address is blank")
  void should_return_empty_when_address_is_blank() {
    // When
    var address = AddressResolver.resolve("   ");

    // Then
    assertThat(address).isEmpty();
  }

  @Test
  @DisplayName("should throw exception when address is unparseable")
  void should_throw_exception_when_address_is_unparseable() {
    assertThatThrownBy(() -> AddressResolver.resolve("unparseable"))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when port is missing")
  void should_throw_exception_when_port_is_missing() {
    assertThatThrownBy(() -> AddressResolver.resolve("192.168.1.1"))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when port is not numeric")
  void should_throw_exception_when_port_is_not_numeric() {
    assertThatThrownBy(() -> AddressResolver.resolve("127.0.0.1:http"))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when port is out of range")
  void should_throw_exception_when_port_is_out_of_range() {
    assertThatThrownBy(() -> AddressResolver.resolve("127.0.0.1:99999"))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when ipv6 missing closing bracket")
  void should_throw_when_ipv6_missing_closing_bracket() {
    assertThatThrownBy(() -> AddressResolver.resolve("[::1:8080"))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should throw exception when http protocol missing port")
  void should_throw_when_protocol_missing_port() {
    assertThatThrownBy(() -> AddressResolver.resolve("http://localhost"))
      .isInstanceOf(IllegalArgumentException.class);
  }
}
