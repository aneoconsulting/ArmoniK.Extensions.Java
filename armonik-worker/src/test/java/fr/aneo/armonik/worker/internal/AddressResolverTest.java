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
