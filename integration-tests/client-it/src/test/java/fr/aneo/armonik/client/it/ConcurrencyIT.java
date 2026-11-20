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

import fr.aneo.armonik.client.ArmoniKClient;
import fr.aneo.armonik.client.BlobCompletionListener;
import fr.aneo.armonik.client.BlobId;
import fr.aneo.armonik.client.SessionHandle;
import fr.aneo.armonik.client.TaskHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/** Several sessions, clients and threads at once, and listeners that call back into the SDK. */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 6, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class ConcurrencyIT {

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  void sixteen_sessions_of_one_client_in_parallel_get_their_own_outputs() {
    try (var client = newClient()) {
      var start = System.nanoTime();
      var futures = new ArrayList<CompletableFuture<Void>>();
      for (int s = 0; s < 16; s++) {
        var index = s;
        futures.add(CompletableFuture.runAsync(() -> roundTrip(client, "c1s" + index + "-", 50), ConcurrencyIT::daemon));
      }
      within(Duration.ofMinutes(5), "16 sessions x 50 tasks", CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)));
      metric("ConcurrencyIT", "sessions16x50_one_client_ms", millisSince(start));
    }
  }

  @Test
  void eight_clients_in_one_jvm_in_parallel_get_their_own_outputs() {
    var clients = new ArrayList<ArmoniKClient>();
    try {
      for (int c = 0; c < 8; c++) clients.add(newClient());
      var start = System.nanoTime();
      var futures = new ArrayList<CompletableFuture<Void>>();
      for (int c = 0; c < clients.size(); c++) {
        for (int s = 0; s < 2; s++) {
          var client = clients.get(c);
          var prefix = "c" + c + "s" + s + "-";
          futures.add(CompletableFuture.runAsync(() -> roundTrip(client, prefix, 25), ConcurrencyIT::daemon));
        }
      }
      within(Duration.ofMinutes(5), "8 clients x 2 sessions x 25 tasks", CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)));
      metric("ConcurrencyIT", "clients8x2x25_ms", millisSince(start));
    } finally {
      clients.forEach(ArmoniKClient::close);
    }
  }

  @Test
  void several_threads_submitting_and_awaiting_on_one_session_all_finish() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var expected = new ConcurrentHashMap<TaskHandle, String>();
      var futures = new ArrayList<CompletableFuture<Void>>();
      for (int t = 0; t < 4; t++) {
        var thread = t;
        futures.add(CompletableFuture.runAsync(() -> {
          for (int i = 0; i < 15; i++) {
            var value = "t" + thread + "i" + i;
            expected.put(session.submitTask(task(session, utf8(value))), value);
            session.awaitOutputsProcessed();
          }
        }, ConcurrencyIT::daemon));
      }
      within(Duration.ofMinutes(5), "4 threads x 15 submit-and-await", CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)));
      assertThat(listener.errors).isEmpty();
      expected.forEach((handle, value) -> assertThat(utf8(listener.data.get(outputId(handle, Duration.ofSeconds(5))))).isEqualTo(value));
    }
  }

  /**
   * A listener that calls back into the SDK and blocks on it: submits a follow-up task and waits for its
   * acknowledgement, and attaches to the session with the blocking getSession. Expected: no deadlock.
   */
  @Test
  void a_listener_that_submits_and_blocks_on_the_sdk_does_not_deadlock() {
    try (var client = newClient()) {
      var sessionRef = new AtomicReference<SessionHandle>();
      var followUps = new AtomicInteger();
      var received = new ConcurrentHashMap<String, Boolean>();
      var failures = new AtomicInteger();
      var listener = new BlobCompletionListener() {
        @Override
        public void onSuccess(Blob blob) {
          var value = utf8(blob.data());
          received.put(value, true);
          if (!value.startsWith("gen0-")) return;
          var session = sessionRef.get();
          var handle = session.submitTask(task(session, utf8("gen1-" + value.substring(5))));
          session.flushSubmissions();
          handle.deferredTaskInfo().toCompletableFuture().join();
          client.getSession(session.sessionInfo().id());
          followUps.incrementAndGet();
        }

        @Override
        public void onError(BlobError blobError) {
          failures.incrementAndGet();
        }
      };
      var session = openSession(client, listener);
      sessionRef.set(session);
      var start = System.nanoTime();
      session.submitTasks(copyTasks(session, 60, "gen0-"));
      run(Duration.ofMinutes(4), "awaitOutputsProcessed with a re-entrant blocking listener", session::awaitOutputsProcessed);
      metric("ConcurrencyIT", "reentrant_listener_120_outputs_ms", millisSince(start));
      assertThat(failures.get()).isZero();
      assertThat(followUps.get()).isEqualTo(60);
      assertThat(received).hasSize(120);
    }
  }

  /**
   * A slow listener in one session must not hold back the outputs of another session of the same
   * client. Session B alone is timed first, then again while session A's listener sleeps 1 s per output.
   */
  @Test
  void a_slow_listener_of_one_session_does_not_delay_another_session() {
    try (var client = newClient()) {
      var alone = timeSession(client, "alone-", 20);

      var slowCalls = new AtomicInteger();
      var slow = new BlobCompletionListener() {
        @Override
        public void onSuccess(Blob blob) {
          slowCalls.incrementAndGet();
          sleep(1_000);
        }

        @Override
        public void onError(BlobError blobError) {
        }
      };
      var slowSession = openSession(client, slow);
      slowSession.submitTasks(copyTasks(slowSession, 40, "slow-"));
      // let the slow session's outputs start to arrive
      await(Duration.ofSeconds(60), () -> slowCalls.get() > 0);
      var busy = timeSession(client, "busy-", 20);
      metric("ConcurrencyIT", "fast_session_alone_ms", alone);
      metric("ConcurrencyIT", "fast_session_next_to_slow_listener_ms", busy);
      run(Duration.ofMinutes(3), "slow session", slowSession::awaitOutputsProcessed);
      assertThat(busy).as("session B next to a slow listener, alone it took %d ms", alone).isLessThan(alone + 5_000);
    }
  }

  /**
   * A listener that blocks (here sleeps 1 s per output) must not slow down task submission elsewhere:
   * another client submits 500 tasks to a paused session, timed alone and next to the slow listener.
   */
  @Test
  void a_slow_listener_does_not_slow_down_submissions_of_another_client() {
    try (var slowClient = newClient(); var other = newClient()) {
      var alone = timeSubmission(other, "alone-");

      var slowCalls = new AtomicInteger();
      var slow = new BlobCompletionListener() {
        @Override
        public void onSuccess(Blob blob) {
          slowCalls.incrementAndGet();
          sleep(1_000);
        }

        @Override
        public void onError(BlobError blobError) {
        }
      };
      var slowSession = openSession(slowClient, slow);
      slowSession.submitTasks(copyTasks(slowSession, 40, "slow-"));
      await(Duration.ofSeconds(60), () -> slowCalls.get() > 0);
      var busy = timeSubmission(other, "busy-");
      metric("ConcurrencyIT", "submit500_alone_ms", alone);
      metric("ConcurrencyIT", "submit500_next_to_slow_listener_ms", busy);
      within(Duration.ofSeconds(30), "cancel", slowSession.cancel());
      assertThat(busy).as("500 submissions next to a slow listener of another client, alone %d ms", alone).isLessThan(alone + 3_000);
    }
  }

  private static long timeSubmission(ArmoniKClient client, String prefix) {
    var session = openSession(client, null);
    within(Duration.ofSeconds(30), "pause", session.pause());
    within(Duration.ofSeconds(60), "library", library(session).libraryHandle().deferredBlobInfo());
    var start = System.nanoTime();
    within(Duration.ofMinutes(2), "500 submissions", allTaskInfos(session.submitTasks(copyTasks(session, 500, prefix))));
    return millisSince(start);
  }

  /** Closing the client from inside a listener call must neither deadlock nor leave waiters hanging. */
  @Test
  void closing_the_client_from_a_listener_returns_and_releases_waiters() {
    var client = newClient();
    var closeMillis = new AtomicReference<Long>();
    var listener = new RecordingListener() {
      @Override
      public void onSuccess(Blob blob) {
        super.onSuccess(blob);
        if (closeMillis.get() == null) {
          var start = System.nanoTime();
          client.close();
          closeMillis.set(millisSince(start));
        }
      }
    };
    var session = openSession(client, listener);
    session.submitTasks(List.of(task(session, utf8("first")), task(session, utf8("second"), "sleep:20000")));
    try {
      run(Duration.ofSeconds(60), "awaitOutputsProcessed while a listener closes the client", session::awaitOutputsProcessed);
    } catch (AssertionError error) {
      if (error.getMessage().contains("not done after")) throw error;
    }
    metric("ConcurrencyIT", "close_from_listener_ms", closeMillis.get());
    assertThat(closeMillis.get()).as("close() called from a listener").isNotNull().isLessThan(10_000L);
  }

  private static long timeSession(ArmoniKClient client, String prefix, int tasks) {
    var start = System.nanoTime();
    roundTrip(client, prefix, tasks);
    return millisSince(start);
  }

  /** Opens a session with its own listener, runs {@code tasks} copies and checks every output. */
  static void roundTrip(ArmoniKClient client, String prefix, int tasks) {
    var listener = new RecordingListener();
    var session = openSession(client, listener);
    var handles = session.submitTasks(copyTasks(session, tasks, prefix));
    session.awaitOutputsProcessed();
    assertThat(listener.errors).as("errors of %s", prefix).isEmpty();
    Map<BlobId, byte[]> data = listener.data;
    for (int i = 0; i < handles.size(); i++) {
      assertThat(utf8(data.get(outputId(handles.get(i), Duration.ofSeconds(5))))).isEqualTo(prefix + i);
    }
  }

  static void daemon(Runnable runnable) {
    var thread = new Thread(runnable, "it-worker");
    thread.setDaemon(true);
    thread.start();
  }
}
