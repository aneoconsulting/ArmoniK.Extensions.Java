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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * Quantifies "one session works great, parallelising is slow": the same number of tasks spread over
 * 1 to 64 sessions of one client, and over several clients. Stress only.
 * <p>
 * Submission-only runs use paused sessions, so they measure the client and the control plane. End to
 * end runs include the workers of the partition, the bottleneck of a small cluster: compare the
 * session counts with each other, not with an absolute target.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#stressEnabled")
@Timeout(value = 20, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ThroughputIT {
  private static final int SUBMIT_TOTAL = 4_000;
  private static final int E2E_TOTAL = 1_920;

  @AfterEach
  void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  @Order(1)
  void submission_rate_by_session_count() {
    var rates = new ArrayList<String>();
    for (int sessions : new int[]{1, 4, 16, 64}) {
      try (var client = newClient()) {
        var handles = new ArrayList<SessionHandle>();
        for (int s = 0; s < sessions; s++) {
          var session = openSession(client, null);
          within(Duration.ofSeconds(30), "pause", session.pause());
          library(session);
          handles.add(session);
        }
        var perSession = SUBMIT_TOTAL / sessions;
        var definitions = handles.stream().map(session -> copyTasks(session, perSession, "sub-")).toList();
        var start = System.nanoTime();
        var acks = new ArrayList<CompletableFuture<Void>>();
        for (int s = 0; s < sessions; s++) {
          var session = handles.get(s);
          var list = definitions.get(s);
          acks.add(CompletableFuture.supplyAsync(() -> session.submitTasks(list), ConcurrencyIT::daemon).thenCompose(ItSupport::allTaskInfos));
        }
        within(Duration.ofMinutes(5), "submission over " + sessions + " sessions", CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)));
        var elapsed = millisSince(start);
        var rate = SUBMIT_TOTAL * 1000L / Math.max(1, elapsed);
        metric("ThroughputIT", "submit_" + SUBMIT_TOTAL + "_over_" + sessions + "_sessions_tasks_per_s", rate);
        rates.add(sessions + ":" + rate);
      }
      cancelCreatedSessions();
    }
    metric("ThroughputIT", "submit_rates", rates);
  }

  @Test
  @Order(2)
  void end_to_end_rate_by_session_count_one_client() {
    var rates = new ArrayList<String>();
    for (int sessions : new int[]{1, 4, 16, 64}) {
      try (var client = newClient()) {
        var rate = endToEnd(List.of(client), sessions);
        metric("ThroughputIT", "e2e_" + E2E_TOTAL + "_over_" + sessions + "_sessions_one_client_tasks_per_s", rate);
        rates.add(sessions + ":" + rate);
      }
      cancelCreatedSessions();
    }
    metric("ThroughputIT", "e2e_rates_one_client", rates);
  }

  @Test
  @Order(3)
  void end_to_end_rate_with_one_client_per_session() {
    var rates = new ArrayList<String>();
    for (int sessions : new int[]{4, 16}) {
      var clients = new ArrayList<ArmoniKClient>();
      try {
        for (int c = 0; c < sessions; c++) clients.add(newClient());
        var rate = endToEnd(clients, sessions);
        metric("ThroughputIT", "e2e_" + E2E_TOTAL + "_over_" + sessions + "_clients_tasks_per_s", rate);
        rates.add(sessions + ":" + rate);
      } finally {
        clients.forEach(ArmoniKClient::close);
      }
      cancelCreatedSessions();
    }
    metric("ThroughputIT", "e2e_rates_client_per_session", rates);
  }

  /** Opens {@code sessions} sessions with a listener, spread over the clients, and times all their tasks end to end. */
  private static long endToEnd(List<ArmoniKClient> clients, int sessions) {
    var perSession = E2E_TOTAL / sessions;
    var listeners = new ArrayList<RecordingListener>();
    var handles = new ArrayList<SessionHandle>();
    for (int s = 0; s < sessions; s++) {
      var listener = new RecordingListener();
      var session = openSession(clients.get(s % clients.size()), listener);
      within(Duration.ofSeconds(60), "library", library(session).libraryHandle().deferredBlobInfo());
      listeners.add(listener);
      handles.add(session);
    }
    var start = System.nanoTime();
    var done = new ArrayList<CompletableFuture<Void>>();
    for (int s = 0; s < sessions; s++) {
      var session = handles.get(s);
      done.add(CompletableFuture.runAsync(() -> {
        session.submitTasks(copyTasks(session, perSession, "e2e-"));
        session.awaitOutputsProcessed();
      }, ConcurrencyIT::daemon));
    }
    within(Duration.ofMinutes(8), "end to end over " + sessions + " sessions", CompletableFuture.allOf(done.toArray(CompletableFuture[]::new)));
    var elapsed = millisSince(start);
    var delivered = listeners.stream().mapToInt(listener -> listener.successes.get()).sum();
    assertThat(delivered).isEqualTo(perSession * sessions);
    return delivered * 1000L / Math.max(1, elapsed);
  }
}
