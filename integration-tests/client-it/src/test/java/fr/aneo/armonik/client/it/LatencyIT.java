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

import fr.aneo.armonik.client.SessionHandle;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * Batching delays: submitTask buffers up to 100 ms, and an output is only
 * watched after a delay. Measures the latency a submit-and-wait round adds on top of the task itself.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 6, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class LatencyIT {
  private static final int ROUNDS = 15;

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  /** Documented: submitTask without flush waits for the 100 ms batching window before the task is sent. */
  @Test
  void submit_task_without_flush_is_acknowledged_after_the_batching_window() {
    try (var client = newClient()) {
      var session = openSession(client, null);
      warmUp(session);
      var total = 0L;
      for (int i = 0; i < ROUNDS; i++) {
        var start = System.nanoTime();
        var handle = session.submitTask(task(session, utf8("ack-" + i)));
        within(Duration.ofSeconds(30), "acknowledgement", handle.deferredTaskInfo());
        total += millisSince(start);
      }
      metric("LatencyIT", "submitTask_ack_avg_ms", total / ROUNDS);
      run(Duration.ofMinutes(1), "drain", session::awaitOutputsProcessed);
    }
  }

  /** Round trip of one task in an idle session: the baseline. */
  @Test
  void a_round_in_an_idle_session() {
    try (var client = newClient()) {
      var session = openSession(client, null);
      warmUp(session);
      metric("LatencyIT", "round_idle_session_avg_ms", averageRound(session, "idle-"));
    }
  }

  /**
   * Rounds in a session where another output stays pending (its task waits for an input that is never
   * written, so it uses no worker): new outputs join the session's existing event stream, which is
   * reopened at most once per second. Expected: no more than 500 ms above the idle baseline.
   */
  @Test
  @Disabled("Fails while a busy session reopens its event stream after 1 s: https://github.com/aneoconsulting/ArmoniK.Extensions.Java/issues/74")
  void a_listener_hears_of_a_new_output_as_fast_in_a_busy_session() throws Exception {
    try (var client = newClient()) {
      var idleListener = new RecordingListener();
      var idle = openSession(client, idleListener);
      warmUp(idle);
      var baselineRounds = listenerRounds(idle, idleListener, "lidle-");
      var baseline = median(baselineRounds);

      var listener = new RecordingListener();
      var session = openSession(client, listener);
      warmUp(session);
      session.submitTask(task(session, fr.aneo.armonik.client.ItTestAccess.neverWrittenBlob(client, session), null));
      session.flushSubmissions();
      sleep(2_000);
      var busyRounds = listenerRounds(session, listener, "lbusy-");
      var busy = median(busyRounds);
      metric("LatencyIT", "listener_round_idle_ms", baselineRounds);
      metric("LatencyIT", "listener_round_busy_ms", busyRounds);
      metric("LatencyIT", "listener_round_idle_median_ms", baseline);
      metric("LatencyIT", "listener_round_busy_median_ms", busy);
      within(Duration.ofSeconds(30), "cancel", session.cancel());
      assertThat(busy).as("median round latency with one pending output in the session, idle median %d ms", baseline).isLessThan(baseline + 500);
    }
  }

  /** Per-round latency from submitTasks to the listener call; the median is compared. */
  private static java.util.List<Long> listenerRounds(SessionHandle session, RecordingListener listener, String prefix) {
    var rounds = new java.util.ArrayList<Long>();
    for (int i = 0; i < ROUNDS; i++) {
      var before = listener.successes.get();
      var start = System.nanoTime();
      session.submitTasks(copyTasks(session, 1, prefix + i + "-"));
      assertThat(await(Duration.ofSeconds(90), () -> listener.successes.get() > before)).as("output %d reported", i).isTrue();
      rounds.add(millisSince(start));
    }
    return rounds;
  }

  private static long median(java.util.List<Long> values) {
    var sorted = values.stream().sorted().toList();
    return sorted.get(sorted.size() / 2);
  }

  private static long averageRound(SessionHandle session, String prefix) {
    var total = 0L;
    for (int i = 0; i < ROUNDS; i++) {
      var start = System.nanoTime();
      session.submitTasks(copyTasks(session, 1, prefix + i + "-"));
      run(Duration.ofSeconds(60), "round", session::awaitOutputsProcessed);
      total += millisSince(start);
    }
    return total / ROUNDS;
  }

  private static void warmUp(SessionHandle session) {
    session.submitTasks(copyTasks(session, 1, "warm-"));
    run(Duration.ofSeconds(60), "warm-up", session::awaitOutputsProcessed);
  }
}
