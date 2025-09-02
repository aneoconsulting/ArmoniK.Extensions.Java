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

import fr.aneo.armonik.client.session.DefaultSessionService;
import fr.aneo.armonik.client.session.Session;
import fr.aneo.armonik.client.task.TaskConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static fr.aneo.armonik.client.session.SessionTestFactory.session;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ArmoniKClientTest {

  @Test
  void should_create_session_when_client_is_created() {
    // Given
    var session = session();
    var services = createServices(session);
    var taskConfiguration = TaskConfiguration.defaultConfiguration();
    var partitionIds = Set.of("partition1");

    // When
    var client = new ArmoniKClient(Set.of("partition1"), taskConfiguration, services);

    // Then
    verify(services.sessions()).createSession(partitionIds, taskConfiguration);
    assertThat(client.session).isEqualTo(session);
  }

  private Services createServices(Session session) {
    var services = mock(Services.class);
    when(services.sessions()).thenReturn(mock(DefaultSessionService.class));
    when(services.sessions().createSession(any(), any())).thenReturn(session);
    return services;
  }
}
