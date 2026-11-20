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

import fr.aneo.armonik.client.SessionId;
import fr.aneo.armonik.client.SessionStatus;
import fr.aneo.armonik.client.TaskConfiguration;
import fr.aneo.armonik.client.TaskHandle;
import fr.aneo.armonik.client.WorkerLibrary;
import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.client.exception.ArmoniKException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/** Functional behaviour against the real control plane: data, graphs, errors, lifecycle. */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 4, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class CorrectnessIT {

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  // ---------------------------------------------------------------- data

  /** Round trip of binary inputs around the upload chunk (84,000 B) and inline (2 MiB) thresholds. */
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 1_000, 83_999, 84_000, 84_001, 168_001, 2 * 1024 * 1024 - 200, 2 * 1024 * 1024 + 1, 9 * 1024 * 1024 + 7})
  void binary_input_round_trips_unchanged(int size) {
    var data = new byte[size];
    new Random(size).nextBytes(data);
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var handle = session.submitTask(task(session, data));
      run(Duration.ofMinutes(3), "awaitOutputsProcessed", session::awaitOutputsProcessed);

      var id = outputId(handle, Duration.ofSeconds(10));
      assertThat(listener.errors).as("errors").isEmpty();
      assertThat(listener.data.get(id)).as("listener data of %d bytes", size).isEqualTo(data);
      assertThat(within(Duration.ofMinutes(1), "download", output(handle).downloadData())).as("downloaded data").isEqualTo(data);
    }
  }

  /** An output larger than the 8 MiB maximum inbound message of the client's channels. */
  @Test
  void large_output_written_by_the_worker_is_downloaded_whole() {
    var size = 20 * 1024 * 1024 + 3;
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var handle = session.submitTask(task(session, utf8("x"), "big:" + size));
      run(Duration.ofMinutes(3), "awaitOutputsProcessed", session::awaitOutputsProcessed);
      var id = outputId(handle, Duration.ofSeconds(10));
      assertThat(listener.errors).isEmpty();
      assertThat(listener.data.get(id)).isEqualTo(pattern(size));
    }
  }

  @Test
  void a_thousand_tasks_each_get_their_own_output() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var start = System.nanoTime();
      var handles = session.submitTasks(copyTasks(session, 1_000, "many-"));
      run(Duration.ofMinutes(3), "awaitOutputsProcessed", session::awaitOutputsProcessed);
      metric("CorrectnessIT", "thousand_tasks_one_session_ms", millisSince(start));
      assertThat(listener.errors).isEmpty();
      for (int i = 0; i < handles.size(); i++) {
        assertThat(utf8(listener.data.get(outputId(handles.get(i), Duration.ofSeconds(5))))).isEqualTo("many-" + i);
      }
    }
  }

  @Test
  void one_uploaded_blob_feeds_many_tasks() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var shared = session.createBlob(InputBlobDefinition.from(utf8("shared")));
      var handles = new ArrayList<TaskHandle>();
      for (int i = 0; i < 10; i++) handles.add(session.submitTask(task(session, shared, "append:" + i)));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed", session::awaitOutputsProcessed);
      for (int i = 0; i < 10; i++) assertThat(utf8(listener.data.get(outputId(handles.get(i), Duration.ofSeconds(5))))).isEqualTo("shared" + i);
    }
  }

  // ---------------------------------------------------------------- graphs

  @Test
  void a_chain_of_ten_tasks_runs_in_order() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      // Each level takes the output handle of the previous one, so the chain is submitted task by task
      var first = session.submitTask(task(session, utf8("x"), "append:0"));
      var previous = output(first);
      var last = first;
      for (int level = 1; level < 10; level++) {
        last = session.submitTask(task(session, previous, "append:" + level));
        previous = output(last);
      }
      run(Duration.ofMinutes(3), "awaitOutputsProcessed of a 10-task chain", session::awaitOutputsProcessed);
      assertThat(listener.errors).isEmpty();
      assertThat(utf8(listener.data.get(outputId(last, Duration.ofSeconds(5))))).isEqualTo("x0123456789");
    }
  }

  @Test
  void a_diamond_waits_for_both_branches() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var root = session.submitTask(task(session, utf8("r"), "sleep:500;append:-root"));
      var left = session.submitTask(task(session, output(root), "append:-left"));
      var right = session.submitTask(task(session, output(root), "sleep:1500;append:-right"));
      var join = session.submitTask(task(session, output(left), "append:-join").withInput("B", output(right)));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed of a diamond", session::awaitOutputsProcessed);
      assertThat(listener.errors).isEmpty();
      assertThat(utf8(listener.data.get(outputId(join, Duration.ofSeconds(5))))).isEqualTo("r-root-left-join");
      assertThat(utf8(listener.data.get(outputId(right, Duration.ofSeconds(5))))).isEqualTo("r-root-right");
    }
  }

  @Test
  void a_subtask_can_complete_the_output_of_its_parent() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var handle = session.submitTask(task(session, utf8("parent"), "sub:-child"));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed of a task delegating to a subtask", session::awaitOutputsProcessed);
      assertThat(listener.errors).as("errors").isEmpty();
      assertThat(utf8(listener.data.get(outputId(handle, Duration.ofSeconds(5))))).isEqualTo("parent-child");
    }
  }

  // ---------------------------------------------------------------- errors and retries

  @ParameterizedTest
  @ValueSource(strings = {"fail", "throw"})
  void a_failing_task_is_reported_to_the_listener_and_does_not_hang_the_wait(String command) {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var start = System.nanoTime();
      var handle = session.submitTask(task(session, utf8("doomed"), command));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed of a failing task", session::awaitOutputsProcessed);
      metric("CorrectnessIT", "failing_task_" + command + "_reported_ms", millisSince(start));
      var id = outputId(handle, Duration.ofSeconds(5));
      assertThat(listener.successes.get()).isZero();
      assertThat(listener.errors).containsKey(id);
      metric("CorrectnessIT", "failing_task_" + command + "_error", listener.errors.get(id).getMessage());
      assertThatThrownBy(() -> within(Duration.ofSeconds(30), "download of an aborted output", output(handle).downloadData()))
        .isInstanceOf(AssertionError.class);
    }
  }

  /** Without a listener the failure is not observable: the wait just returns. */
  @Test
  void a_failing_task_without_listener_does_not_hang_the_wait() {
    try (var client = newClient()) {
      var session = openSession(client, null);
      session.submitTask(task(session, utf8("doomed"), "fail"));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed of a failing task without listener", session::awaitOutputsProcessed);
    }
  }

  /** ArmoniK retries a task when its worker crashes, not when it returns an error: the error is final. */
  @Test
  void a_business_error_is_final_even_with_retries_left() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener, new TaskConfiguration(5, 1, PARTITION, Duration.ofMinutes(5), new HashMap<>()));
      var handle = session.submitTask(task(session, utf8("retry"), "failonce:" + UUID.randomUUID() + ";copy"));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed of a failing task", session::awaitOutputsProcessed);
      assertThat(listener.errors).containsKey(outputId(handle, Duration.ofSeconds(5)));
    }
  }

  /** The worker JVM halts during the first attempt: the control plane retries the task, the client sees one success. */
  @Test
  void a_task_whose_worker_crashes_is_retried_and_its_output_delivered_once() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener, new TaskConfiguration(5, 1, PARTITION, Duration.ofMinutes(5), new HashMap<>()));
      var start = System.nanoTime();
      var handle = session.submitTask(task(session, utf8("survivor"), "crashonce:" + UUID.randomUUID() + ";copy"));
      run(Duration.ofMinutes(3), "awaitOutputsProcessed of a task retried after a worker crash", session::awaitOutputsProcessed);
      metric("CorrectnessIT", "crash_retry_round_trip_ms", millisSince(start));
      var id = outputId(handle, Duration.ofSeconds(5));
      assertThat(listener.errors).as("errors").isEmpty();
      assertThat(listener.successes.get()).isEqualTo(1);
      assertThat(utf8(listener.data.get(id))).isEqualTo("survivor");
    }
  }

  @Test
  void a_task_without_worker_library_fails_instead_of_hanging() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var handle = session.submitTask(new TaskDefinition().withInput("A", InputBlobDefinition.from(utf8("x"))).withOutput("result")
                                                          .withWorkerLibrary(WorkerLibrary.NO_WORKER_LIBRARY));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed of a task the dynamic worker cannot load", session::awaitOutputsProcessed);
      assertThat(listener.errors).containsKey(outputId(handle, Duration.ofSeconds(5)));
    }
  }

  // ---------------------------------------------------------------- session lifecycle

  @Test
  void cancelling_a_session_reports_its_running_outputs_as_failed() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      var handles = session.submitTasks(List.of(task(session, utf8("a"), "sleep:20000"), task(session, utf8("b"), "sleep:20000"), task(session, utf8("c"), "sleep:20000")));
      within(Duration.ofSeconds(30), "acknowledgement", allTaskInfos(handles));
      sleep(2_000);
      var start = System.nanoTime();
      var state = within(Duration.ofSeconds(30), "cancel", session.cancel());
      assertThat(state.status()).isEqualTo(SessionStatus.CANCELLED);
      run(Duration.ofSeconds(60), "awaitOutputsProcessed after cancel", session::awaitOutputsProcessed);
      metric("CorrectnessIT", "cancel_to_outputs_reported_ms", millisSince(start));
      assertThat(listener.failures.get()).isEqualTo(3);
    }
  }

  @Test
  void a_paused_session_runs_its_tasks_once_resumed() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      assertThat(within(Duration.ofSeconds(30), "pause", session.pause()).status()).isEqualTo(SessionStatus.PAUSED);
      session.submitTasks(copyTasks(session, 5, "paused-"));
      sleep(3_000);
      assertThat(listener.events()).as("outputs of a paused session").isZero();
      assertThat(within(Duration.ofSeconds(30), "resume", session.resume()).status()).isEqualTo(SessionStatus.RUNNING);
      run(Duration.ofMinutes(2), "awaitOutputsProcessed after resume", session::awaitOutputsProcessed);
      assertThat(listener.successes.get()).isEqualTo(5);
    }
  }

  @Test
  void a_task_submitted_to_a_closed_session_fails_and_is_reported() {
    try (var client = newClient()) {
      var listener = new RecordingListener();
      var session = openSession(client, listener);
      within(Duration.ofSeconds(30), "library upload", library(session).libraryHandle().deferredBlobInfo());
      assertThat(within(Duration.ofSeconds(30), "close", session.close()).status()).isEqualTo(SessionStatus.CLOSED);
      var handle = session.submitTask(task(session, utf8("late")));
      session.flushSubmissions();
      assertThatThrownBy(() -> within(Duration.ofSeconds(30), "task info", handle.deferredTaskInfo())).isInstanceOf(AssertionError.class)
                                                                                                    .hasMessageNotContaining("not done after");
      run(Duration.ofSeconds(30), "awaitOutputsProcessed after a rejected task", session::awaitOutputsProcessed);
      assertThat(listener.failures.get()).isEqualTo(1);
    }
  }

  @Test
  void a_finished_session_can_be_purged_and_deleted() {
    try (var client = newClient()) {
      var session = openSession(client, null);
      session.submitTasks(copyTasks(session, 3, "purge-"));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed", session::awaitOutputsProcessed);
      within(Duration.ofSeconds(30), "close", session.close());
      assertThat(within(Duration.ofSeconds(30), "purge", session.purge()).status()).isEqualTo(SessionStatus.PURGED);
      assertThat(within(Duration.ofSeconds(30), "delete", session.delete()).status()).isEqualTo(SessionStatus.DELETED);
      var id = session.sessionInfo().id();
      assertThatThrownBy(() -> call(Duration.ofSeconds(30), "getSession of a deleted session", () -> client.getSession(id)))
        .hasRootCauseInstanceOf(io.grpc.StatusRuntimeException.class);
    }
  }

  @Test
  void another_client_can_attach_to_a_session_and_receive_its_new_outputs() {
    try (var first = newClient(); var second = newClient()) {
      var session = openSession(first, null);
      var listener = new RecordingListener();
      var attached = call(Duration.ofSeconds(30), "getSession", () -> second.getSession(session.sessionInfo().id(), listener));
      var handle = attached.submitTask(task(attached, utf8("attached")));
      run(Duration.ofMinutes(2), "awaitOutputsProcessed", attached::awaitOutputsProcessed);
      assertThat(utf8(listener.data.get(outputId(handle, Duration.ofSeconds(5))))).isEqualTo("attached");
    }
  }

  // ---------------------------------------------------------------- invalid requests

  @Test
  void opening_a_session_on_an_unknown_partition_fails_fast() {
    try (var client = newClient()) {
      var start = System.nanoTime();
      assertThatThrownBy(() -> call(Duration.ofSeconds(60), "openSession on an unknown partition",
        () -> client.openSession(new SessionDefinition(Set.of("NoSuchPartition-" + UUID.randomUUID())))))
        .isInstanceOf(AssertionError.class).hasMessageNotContaining("not done after");
      metric("CorrectnessIT", "unknown_partition_failure_ms", millisSince(start));
    }
  }

  @Test
  void attaching_to_an_unknown_session_fails_with_an_armonik_exception() {
    try (var client = newClient()) {
      assertThatThrownBy(() -> call(Duration.ofSeconds(60), "getSession of an unknown session",
        () -> client.getSession(SessionId.from(UUID.randomUUID().toString()))))
        .isInstanceOf(AssertionError.class).hasMessageNotContaining("not done after").hasCauseInstanceOf(ArmoniKException.class);
    }
  }
}
