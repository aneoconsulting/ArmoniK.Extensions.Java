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
package fr.aneo.armonik.client.definition;

import fr.aneo.armonik.client.TaskConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SessionDefinitionTest {
  @Test
  void should_throw_exception_when_task_partition_is_not_in_session_partitions() {
    // Given
    var partitions = Set.of("partition_1", "partition_2");
    var taskConfig = new TaskConfiguration(2, 1, "partition_3", Duration.ofMinutes(60), Map.of());

    // When / then
    assertThatThrownBy(() -> new SessionDefinition(partitions, taskConfig))
      .isInstanceOf(IllegalArgumentException.class);

  }

  @Test
  void should_use_default_task_configuration_when_none_specified() {
    // Given
    var partitions = Set.of("partition_1", "partition_2");

    // When
    var sessionDefinition = new SessionDefinition(partitions);

    // Then
    assertThat(sessionDefinition.taskConfiguration()).isEqualTo(TaskConfiguration.defaultConfiguration());
  }

}
