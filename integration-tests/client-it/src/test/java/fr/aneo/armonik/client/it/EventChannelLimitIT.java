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

import fr.aneo.armonik.client.ArmoniKConfig;
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
 * Output watches of sessions whose outputs stay pending must not hold every channel of the client.
 * <p>
 * Expected: sessions whose outputs stay pending (paused) never prevent another session of the same
 * client from being opened, submitting, and having its outputs watched and processed.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 6, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EventChannelLimitIT {

  @AfterEach
  void cleanUp() {
    cancelCreatedSessions();
  }

  /** 10 paused sessions with 500 pending tasks each, then an 11th session. */
  @Test
  @Order(1)
  void ten_paused_sessions_with_500_pending_tasks_do_not_block_an_eleventh() {
    try (var client = newClient()) {
      pausedSessionsWithPendingTasks(client, 10, 500);

      var start = System.nanoTime();
      var other = call(Duration.ofSeconds(15), "open an 11th session and submit a task", () -> {
        var session = openSession(client, null);
        var handle = session.submitTask(task(session, utf8("eleventh")));
        session.flushSubmissions();
        handle.deferredTaskInfo().toCompletableFuture().get(15, TimeUnit.SECONDS);
        return session;
      });
      metric("EventChannelLimitIT", "eleventh_session_acknowledged_ms", millisSince(start));
      run(Duration.ofSeconds(60), "awaitOutputsProcessed of the 11th session", other::awaitOutputsProcessed);
      metric("EventChannelLimitIT", "eleventh_session_processed_ms", millisSince(start));
    }
  }

  /**
   * One GetEvents stream per session (up to 1000 outputs each) holds a channel of the
   * event pool, 50 by default. With more sessions waiting on outputs than event channels, the outputs
   * of a new session are not watched until another session's outputs all complete.
   */
  @Test
  @Order(2)
  void more_paused_sessions_than_event_channels_do_not_block_the_outputs_of_another_session() {
    var eventChannels = ArmoniKConfig.DEFAULT_EVENT_CHANNEL_POOL_SIZE;
    try (var client = newClient()) {
      pausedSessionsWithPendingTasks(client, eventChannels + 5, 1);
      metric("EventChannelLimitIT", "event_channels_in_use_with_" + (eventChannels + 5) + "_paused_sessions", channelsInUse(client, "eventChannelPool"));

      var start = System.nanoTime();
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      session.submitTask(task(session, utf8("late")));
      try {
        run(Duration.ofSeconds(60), "awaitOutputsProcessed of a running session while " + (eventChannels + 5) + " paused sessions wait",
          session::awaitOutputsProcessed);
      } finally {
        metric("EventChannelLimitIT", "late_session_outputs_delivered", listener.events());
      }
      metric("EventChannelLimitIT", "late_session_processed_ms", millisSince(start));
      assertThat(listener.successes.get()).isEqualTo(1);
    }
  }

  private static void pausedSessionsWithPendingTasks(fr.aneo.armonik.client.ArmoniKClient client, int sessions, int tasksPerSession) {
    var acknowledged = new ArrayList<CompletableFuture<Void>>();
    var handles = new ArrayList<SessionHandle>();
    for (int i = 0; i < sessions; i++) {
      var session = openSession(client, null);
      within(Duration.ofSeconds(30), "pause", session.pause());
      handles.add(session);
      acknowledged.add(allTaskInfos(session.submitTasks(copyTasks(session, tasksPerSession, "pinned-" + i + "-"))));
    }
    within(Duration.ofMinutes(3), "acknowledgement of the paused sessions' tasks", CompletableFuture.allOf(acknowledged.toArray(CompletableFuture[]::new)));
    // Output watches open 50 ms after the tasks are acknowledged
    sleep(3_000);
  }
}
