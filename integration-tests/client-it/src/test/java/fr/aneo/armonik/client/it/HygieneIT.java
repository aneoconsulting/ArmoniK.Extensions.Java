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

import fr.aneo.armonik.client.SessionState;
import fr.aneo.armonik.client.TaskHandle;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/** Resources left behind by clients: threads, connections, and work still pending at close. */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 5, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HygieneIT {

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  @Order(1)
  void clients_opened_and_closed_in_a_loop_leak_neither_threads_nor_connections() {
    // Warm up: gRPC's shared event loops and the SDK's static executors are created once per JVM
    try (var warmUp = newClient()) {
      ConcurrencyIT.roundTrip(warmUp, "warm-", 3);
    }
    sleep(1_000);
    var baselineThreads = liveThreads().size();
    var baselineNonDaemon = nonDaemonThreads();

    var closeTimes = new ArrayList<Long>();
    for (int i = 0; i < 10; i++) {
      var client = newClient();
      ConcurrencyIT.roundTrip(client, "loop" + i + "-", 5);
      var start = System.nanoTime();
      client.close();
      closeTimes.add(millisSince(start));
    }
    await(Duration.ofSeconds(5), () -> connectionsTo(endpointPort()) == 0);
    sleep(2_000);

    var threadNames = liveThreads().stream().map(Thread::getName).map(name -> name.replaceAll("\\d+", "N"))
                                   .collect(Collectors.groupingBy(name -> name, Collectors.counting()));
    metric("HygieneIT", "close_ms_each", closeTimes);
    metric("HygieneIT", "threads_before_after", baselineThreads + "->" + liveThreads().size());
    metric("HygieneIT", "non_daemon_threads_before_after", baselineNonDaemon + "->" + nonDaemonThreads());
    metric("HygieneIT", "thread_names_after", threadNames);
    metric("HygieneIT", "connections_after", connectionsTo(endpointPort()));

    assertThat(connectionsTo(endpointPort())).as("TCP connections left after 10 closed clients").isZero();
    assertThat(nonDaemonThreads()).as("non-daemon threads").isLessThanOrEqualTo(baselineNonDaemon);
    assertThat(liveThreads().size()).as("threads after 10 clients, %s", threadNames).isLessThanOrEqualTo(baselineThreads + 8);
    assertThat(closeTimes).allMatch(millis -> millis < 1_000, "close() under 1 s");
  }

  /** close() with outputs still awaited: returns at once, reports them failed, and releases waiters. */
  @Test
  @Order(2)
  void closing_with_outputs_pending_reports_them_and_releases_the_waiters() {
    var client = newClient();
    var listener = new RecordingListener();
    var session = openSession(client, listener);
    within(Duration.ofSeconds(30), "pause", session.pause());
    var handles = session.submitTasks(copyTasks(session, 5, "pending-"));
    within(Duration.ofSeconds(30), "acknowledgement", allTaskInfos(handles));
    sleep(2_000);
    var waiter = CompletableFuture.runAsync(session::awaitOutputsProcessed, ConcurrencyIT::daemon);

    var start = System.nanoTime();
    client.close();
    var closeMillis = millisSince(start);
    metric("HygieneIT", "close_with_pending_outputs_ms", closeMillis);
    within(Duration.ofSeconds(10), "awaitOutputsProcessed after close", waiter.handle((ignored, error) -> null));
    assertThat(closeMillis).isLessThan(2_000);
    assertThat(listener.failures.get()).as("outputs reported failed at close").isEqualTo(5);
  }

  /** close() while uploads and submissions are in flight: every handle completes, none is left hanging. */
  @Test
  @Order(3)
  void closing_during_a_submission_completes_every_handle() {
    var client = newClient();
    var session = openSession(client, null);
    within(Duration.ofSeconds(30), "pause", session.pause());
    var library = library(session);
    var random = new Random(3);
    var definitions = new ArrayList<TaskDefinition>();
    for (int i = 0; i < 40; i++) {
      var data = new byte[2_500_000];
      random.nextBytes(data);
      definitions.add(new TaskDefinition().withInput("A", InputBlobDefinition.from(data)).withOutput("result").withWorkerLibrary(library));
    }
    var handles = session.submitTasks(definitions);
    sleep(300);
    var start = System.nanoTime();
    client.close();
    metric("HygieneIT", "close_during_submission_ms", millisSince(start));
    var settled = CompletableFuture.allOf(handles.stream().map(TaskHandle::deferredTaskInfo)
                                                 .map(stage -> stage.toCompletableFuture().handle((info, error) -> null))
                                                 .toArray(CompletableFuture[]::new));
    within(Duration.ofSeconds(30), "every task handle settled after close", settled);
    assertThat(millisSince(start)).isLessThan(30_000);
  }

  /**
   * After a burst of 200 concurrent calls, how many connections stay open. Recorded only: the pool
   * reuses the most recent channel first and shuts idle ones after 5 minutes.
   */
  @Test
  @Order(4)
  void connections_after_a_burst() {
    try (var client = newClient()) {
      var session = openSession(client, null);
      var calls = new ArrayList<CompletableFuture<SessionState>>();
      for (int i = 0; i < 200; i++) calls.add(session.resume().toCompletableFuture());
      within(Duration.ofSeconds(60), "200 concurrent resume", CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)));
      var afterBurst = connectionsTo(endpointPort());
      for (int i = 0; i < 50; i++) within(Duration.ofSeconds(10), "sequential resume", session.resume());
      sleep(5_000);
      metric("HygieneIT", "connections_after_burst", afterBurst);
      metric("HygieneIT", "connections_after_sequential_calls_and_5s_idle", connectionsTo(endpointPort()));
      metric("HygieneIT", "pool_channels_available_after_burst", poolState(client, "channelPool")[0] + "/" + poolState(client, "channelPool")[1]);
    }
  }

  /**
   * Mail "other finding" 4: after a burst, connections must idle out. The pool reuses the most recent
   * channel first and shuts down channels idle for 5 minutes. One call per second for 6 minutes after
   * a burst of 200 concurrent calls should leave about one connection. Stress only (6 minutes).
   */
  @Test
  @Order(5)
  @EnabledIf("fr.aneo.armonik.client.it.ItSupport#stressEnabled")
  @Timeout(value = 10, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
  void connections_opened_by_a_burst_idle_out() {
    try (var client = newClient()) {
      var session = openSession(client, null);
      var calls = new ArrayList<CompletableFuture<SessionState>>();
      for (int i = 0; i < 200; i++) calls.add(session.resume().toCompletableFuture());
      within(Duration.ofSeconds(60), "200 concurrent resume", CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)));
      var afterBurst = connectionsTo(endpointPort());
      var deadline = System.nanoTime() + Duration.ofMinutes(6).toNanos();
      while (System.nanoTime() < deadline) {
        within(Duration.ofSeconds(10), "sequential resume", session.resume());
        sleep(1_000);
      }
      var after = connectionsTo(endpointPort());
      metric("HygieneIT", "idle_out_connections_after_burst", afterBurst);
      metric("HygieneIT", "idle_out_connections_after_6min_of_1_call_per_s", after);
      assertThat(after).as("connections left 6 min after a burst of %d", afterBurst).isLessThanOrEqualTo(3);
    }
  }
}
