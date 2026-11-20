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
import fr.aneo.armonik.client.TaskHandle;
import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/** Unreachable and unresponsive control planes, timeouts, and a control plane restart under load. */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 8, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class FailureIT {

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  void opening_a_session_on_a_refused_port_fails_within_a_minute() throws IOException {
    int port;
    try (var socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    try (var client = newClient("http://localhost:" + port, builder -> {
    })) {
      var start = System.nanoTime();
      assertThatThrownBy(() -> call(Duration.ofSeconds(60), "openSession on a refused port",
        () -> client.openSession(new SessionDefinition(Set.of(PARTITION)))))
        .isInstanceOf(AssertionError.class).hasMessageNotContaining("not done after");
      metric("FailureIT", "refused_port_failure_ms", millisSince(start));
    }
  }

  /** A server that accepts connections and never answers: no call has a deadline, so does it hang? */
  @Test
  void opening_a_session_on_a_silent_server_fails_within_a_minute() throws Exception {
    try (var server = new ServerSocket(0)) {
      var accepted = new ArrayList<Socket>();
      var acceptor = new Thread(() -> {
        while (!server.isClosed()) {
          try {
            accepted.add(server.accept());
          } catch (IOException e) {
            return;
          }
        }
      }, "it-silent-server");
      acceptor.setDaemon(true);
      acceptor.start();
      var client = newClient("http://localhost:" + server.getLocalPort(), builder -> {
      });
      try {
        var start = System.nanoTime();
        try {
          call(Duration.ofSeconds(60), "openSession on a server that never answers",
            () -> client.openSession(new SessionDefinition(Set.of(PARTITION))));
        } catch (AssertionError expected) {
          metric("FailureIT", "silent_server_outcome", expected.getMessage().contains("not done after") ? "hang>60s" : "failed after " + millisSince(start) + " ms");
          assertThat(expected.getMessage()).as("openSession on a silent server").doesNotContain("not done after");
        }
      } finally {
        server.close();
        accepted.forEach(socket -> {
          try {
            socket.close();
          } catch (IOException ignored) {
            // closing
          }
        });
        client.close();
      }
    }
  }

  /** PR #70 adds awaitOutputsProcessed(Duration); skipped where the method does not exist. */
  @Test
  void a_timed_wait_gives_up_and_a_later_wait_still_succeeds() throws Exception {
    var timed = findTimedAwait();
    Assumptions.assumeTrue(timed != null, "SessionHandle.awaitOutputsProcessed(Duration) not in this version");
    try (var client = newClient()) {
      var session = openSession(client, null);
      within(Duration.ofSeconds(30), "pause", session.pause());
      session.submitTask(task(session, utf8("timed")));
      var start = System.nanoTime();
      var finished = (Boolean) timed.invoke(session, Duration.ofSeconds(2));
      var elapsed = millisSince(start);
      metric("FailureIT", "timed_wait_2s_elapsed_ms", elapsed);
      assertThat(finished).isFalse();
      assertThat(elapsed).isBetween(1_900L, 5_000L);
      within(Duration.ofSeconds(30), "resume", session.resume());
      assertThat((Boolean) call(Duration.ofSeconds(90), "timed wait after resume", () -> timed.invoke(session, Duration.ofSeconds(60)))).isTrue();
    }
  }

  private static java.lang.reflect.Method findTimedAwait() {
    try {
      return SessionHandle.class.getMethod("awaitOutputsProcessed", Duration.class);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  /**
   * Restarts the control plane container while 4 sessions loop on rounds of 3 tasks: one 2.5 MB
   * input (streamed upload) and two 300 KB inputs (inline). Expected: every output that is delivered
   * equals its input (no corrupted replay of an upload), the rounds resume after the restart, and
   * nothing hangs. Failures caused by the restart are recorded.
   */
  @Test
  @EnabledIf("fr.aneo.armonik.client.it.ItSupport#restartEnabled")
  @Timeout(value = 12, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
  void a_control_plane_restart_under_load_neither_corrupts_data_nor_stalls() throws Exception {
    var stop = new AtomicBoolean();
    var rounds = new AtomicInteger();
    var roundsAfterRestart = new AtomicInteger();
    var restarted = new AtomicBoolean();
    var corrupted = new AtomicInteger();
    var delivered = new AtomicInteger();
    var listenerErrors = new AtomicInteger();
    var roundFailures = new AtomicInteger();
    var firstFailure = new AtomicReference<Throwable>();
    var lastProgress = new AtomicLong(System.nanoTime());
    try (var client = newClient()) {
      var threads = new ArrayList<Thread>();
      for (int s = 0; s < 4; s++) {
        var seed = s;
        var thread = new Thread(() -> {
          var random = new Random(seed);
          Map<String, byte[]> expected = new ConcurrentHashMap<>();
          var listener = new RecordingListener() {
            @Override
            public void onSuccess(Blob blob) {
              super.onSuccess(blob);
              delivered.incrementAndGet();
              var want = expected.get(blob.blobInfo().id().asString());
              if (want != null && !java.util.Arrays.equals(want, blob.data())) corrupted.incrementAndGet();
            }

            @Override
            public void onError(BlobError blobError) {
              super.onError(blobError);
              listenerErrors.incrementAndGet();
            }
          };
          SessionHandle session = null;
          while (!stop.get()) {
            try {
              if (session == null) session = openSession(client, listener);
              var definitions = new ArrayList<TaskDefinition>();
              var inputs = new ArrayList<byte[]>();
              for (int size : new int[]{2_500_000, 300_000, 300_000}) {
                var data = new byte[size];
                random.nextBytes(data);
                inputs.add(data);
                definitions.add(new TaskDefinition().withInput("A", InputBlobDefinition.from(data)).withOutput("result")
                                                    .withWorkerLibrary(library(session)));
              }
              List<TaskHandle> handles = session.submitTasks(definitions);
              for (int i = 0; i < handles.size(); i++) {
                var index = i;
                output(handles.get(i)).deferredBlobInfo().thenAccept(info -> expected.put(info.id().asString(), inputs.get(index)));
              }
              session.awaitOutputsProcessed();
              rounds.incrementAndGet();
              if (restarted.get()) roundsAfterRestart.incrementAndGet();
              lastProgress.set(System.nanoTime());
            } catch (Throwable throwable) {
              roundFailures.incrementAndGet();
              firstFailure.compareAndSet(null, throwable);
              sleep(1_000);
            }
          }
        }, "it-restart-load-" + s);
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();
      }

      sleep(15_000);
      var roundsBefore = rounds.get();
      var process = new ProcessBuilder("docker", "restart", RESTART_CONTAINER).redirectErrorStream(true).start();
      assertThat(process.waitFor(2, TimeUnit.MINUTES)).as("docker restart").isTrue();
      restarted.set(true);
      var restartedAt = System.nanoTime();
      // Run until 20 rounds completed after the restart, or 3 minutes
      await(Duration.ofMinutes(3), () -> roundsAfterRestart.get() >= 20);
      var recoveryMillis = millisSince(restartedAt);
      stop.set(true);
      for (var thread : threads) thread.join(60_000);
      var stuck = threads.stream().filter(Thread::isAlive).count();

      metric("FailureIT", "restart_rounds_before_after", roundsBefore + "/" + roundsAfterRestart.get());
      metric("FailureIT", "restart_recovery_ms", recoveryMillis);
      metric("FailureIT", "restart_delivered", delivered.get());
      metric("FailureIT", "restart_corrupted_outputs", corrupted.get());
      metric("FailureIT", "restart_listener_errors", listenerErrors.get());
      metric("FailureIT", "restart_round_failures", roundFailures.get());
      metric("FailureIT", "restart_first_failure", firstFailure.get());
      metric("FailureIT", "restart_threads_stuck_after_stop", stuck);

      assertThat(corrupted.get()).as("outputs whose data differs from their input").isZero();
      assertThat(roundsAfterRestart.get()).as("rounds completed in 3 min after the restart. %s", sdkThreads()).isGreaterThanOrEqualTo(20);
      assertThat(stuck).as("round threads still blocked 60 s after the end. %s", sdkThreads()).isZero();
    }
  }
}
