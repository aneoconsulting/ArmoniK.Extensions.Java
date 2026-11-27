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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * No deadlock with listeners, run in its own JVM (one fork per class).
 * <p>
 * Sessions with a listener are paused, all their tasks submitted so that every output watch opens
 * before any output completes, then resumed from a second client. Expected: every output reaches
 * the listener, and delivery never stops for 60 s.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 30, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ListenerDeadlockIT {
  private static final Duration NO_PROGRESS_LIMIT = Duration.ofSeconds(60);

  @AfterEach
  void cleanUp() {
    cancelCreatedSessions();
  }

  /** 12 sessions x 600 tasks, stress only. */
  @Test
  @Order(1)
  @EnabledIf("fr.aneo.armonik.client.it.ItSupport#stressEnabled")
  void twelve_paused_sessions_with_listeners_receive_every_output_once_resumed() throws Exception {
    deliverAll(12, 600);
  }

  /** More sessions with a listener than event channels (50): each holds one GetEvents stream. */
  @Test
  @Order(2)
  void sixty_paused_sessions_with_listeners_receive_every_output_once_resumed() throws Exception {
    deliverAll(60, 10);
  }

  private static void deliverAll(int sessionCount, int tasksPerSession) throws Exception {
    var total = sessionCount * tasksPerSession;
    var listener = new RecordingListener();
    var test = "deliverAll_" + sessionCount + "x" + tasksPerSession;
    try (var client = newClient()) {
      var sessions = new ArrayList<SessionHandle>();
      for (int i = 0; i < sessionCount; i++) {
        var session = openSession(client, listener);
        within(Duration.ofSeconds(30), "pause", session.pause());
        sessions.add(session);
      }
      var acknowledged = sessions.stream()
                                 .map(session -> allTaskInfos(session.submitTasks(copyTasks(session, tasksPerSession, "dl-"))))
                                 .toArray(CompletableFuture[]::new);
      within(Duration.ofMinutes(3), "submission to the paused sessions", CompletableFuture.allOf(acknowledged));
      sleep(3_000);
      metric("ListenerDeadlockIT", test + "_event_channels_in_use_while_paused", channelsInUse(client, "eventChannelPool"));

      try (var admin = newClient()) {
        for (var session : sessions) {
          within(Duration.ofSeconds(30), "resume", admin.getSession(session.sessionInfo().id()).resume());
        }
      }

      var start = System.nanoTime();
      listener.lastEventNanos.set(start);
      var processed = CompletableFuture.allOf(sessions.stream()
                                                      .map(session -> CompletableFuture.runAsync(session::awaitOutputsProcessed, runnable -> {
                                                        var thread = new Thread(runnable, "it-await");
                                                        thread.setDaemon(true);
                                                        thread.start();
                                                      }))
                                                      .toArray(CompletableFuture[]::new));
      while (!processed.isDone() && System.nanoTime() - listener.lastEventNanos.get() < NO_PROGRESS_LIMIT.toNanos()) {
        Thread.sleep(1_000);
      }
      var elapsed = millisSince(start);
      metric("ListenerDeadlockIT", test + "_delivered", listener.events() + "/" + total);
      metric("ListenerDeadlockIT", test + "_outputs_per_s", String.format("%.1f", listener.events() * 1000.0 / Math.max(1, elapsed)));
      metric("ListenerDeadlockIT", test + "_listener_threads", listener.threads);

      assertThat(processed.isDone()).as("delivery stopped for %d s with %d/%d delivered. %s",
        NO_PROGRESS_LIMIT.toSeconds(), listener.events(), total, sdkThreads()).isTrue();
      assertThat(listener.failures.get()).as("outputs reported as failed: %s", listener.errors.values().stream().findFirst()).isZero();
      assertThat(listener.successes.get()).isEqualTo(total);
    }
  }
}
