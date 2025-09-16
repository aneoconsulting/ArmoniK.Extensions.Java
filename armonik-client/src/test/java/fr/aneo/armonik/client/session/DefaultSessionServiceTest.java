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
package fr.aneo.armonik.client.session;

import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static fr.aneo.armonik.client.task.TaskConfiguration.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class DefaultSessionServiceTest {

  @Test
  void should_throw_exception_when_default_task_partition_is_not_in_sessions_partitions() {
    // Given
    var taskConfiguration = defaultConfigurationWithPartition("partition2");
    var sessionPartitions = Set.of("partition1");
    var sessionService = new DefaultSessionService(mock(ManagedChannel.class));

    // When / Then
    assertThatThrownBy(() -> sessionService.createSession(sessionPartitions, taskConfiguration)).isInstanceOf(IllegalArgumentException.class);
  }
}
