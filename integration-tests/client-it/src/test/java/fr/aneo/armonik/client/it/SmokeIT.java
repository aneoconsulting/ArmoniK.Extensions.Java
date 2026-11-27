/*
 * Copyright (C) 2025 ANEO (armonik@aneo.fr)
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
package fr.aneo.armonik.client.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.time.Duration;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/** Checks that the partition runs the test processors before anything heavier. */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 3, unit = java.util.concurrent.TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class SmokeIT {

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  void worker_copies_input_A_to_output_result() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var start = System.nanoTime();
      var handle = session.submitTask(task(session, utf8("hello")));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed of one task", session::awaitOutputsProcessed);
      metric("SmokeIT", "one_task_round_trip_ms", millisSince(start));

      var id = outputId(handle, Duration.ofSeconds(10));
      assertThat(listener.errors).as("errors reported").isEmpty();
      assertThat(utf8(listener.data.get(id))).isEqualTo("hello");
      assertThat(utf8(within(Duration.ofSeconds(30), "download", output(handle).downloadData()))).isEqualTo("hello");
    }
  }
}
