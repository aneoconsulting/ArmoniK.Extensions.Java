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
package fr.aneo.armonik.client;

import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.testutils.*;
import io.grpc.BindableService;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static fr.aneo.armonik.client.TestDataFactory.blobHandle;
import static fr.aneo.armonik.client.TestDataFactory.sessionInfo;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskSubmissionBufferingTest extends InProcessGrpcTestBase {
  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private final TasksGrpcMock tasksGrpcMock = new TasksGrpcMock();
  private final SessionsGrpcMock sessionsGrpcMock = new SessionsGrpcMock();
  private DeterministicScheduler scheduler;
  private SessionInfo sessionInfo;

  @Override
  protected List<BindableService> services() {
    return List.of(resultsGrpcMock, tasksGrpcMock, sessionsGrpcMock);
  }

  @BeforeEach
  void setUp() {
    scheduler = new DeterministicScheduler();
    sessionInfo = sessionInfo("partition_1", new TaskConfiguration(3, 1, "partition_1", Duration.ofMinutes(60), Map.of()));
    resultsGrpcMock.reset();
  }

  @Test
  @DisplayName("should buffer submitted tasks until the batching window elapses")
  void should_buffer_tasks_until_window_elapses() {
    // Given
    var session = sessionHandle(new TaskSubmissionBatching(10, Duration.ofMillis(100)));
    var handles = List.of(session.submitTask(task()), session.submitTask(task()), session.submitTask(task()));

    // Then: nothing is sent before the window
    scheduler.tick(99, MILLISECONDS);
    assertThat(resultsGrpcMock.metadataCalls).isZero();
    assertThat(handles).noneSatisfy(handle -> assertThat(handle.deferredTaskInfo().toCompletableFuture()).isDone());

    // When
    scheduler.tick(1, MILLISECONDS);
    handles.forEach(handle -> handle.deferredTaskInfo().toCompletableFuture().join());

    // Then
    assertThat(resultsGrpcMock.metadataCalls).isEqualTo(1);
    assertThat(tasksGrpcMock.submitTasksRequests).hasSize(1);
    assertThat(tasksGrpcMock.submittedTasksRequest.getTaskCreationsCount()).isEqualTo(3);
  }

  @Test
  @DisplayName("should send a batch as soon as the buffer holds batchSize tasks")
  void should_send_batch_when_buffer_is_full() {
    // Given
    var session = sessionHandle(new TaskSubmissionBatching(2, Duration.ofMillis(100)));

    // When
    var first = session.submitTask(task());
    var second = session.submitTask(task());
    var third = session.submitTask(task());
    first.deferredTaskInfo().toCompletableFuture().join();
    second.deferredTaskInfo().toCompletableFuture().join();

    // Then: the first two are sent without the timer, the third waits
    assertThat(tasksGrpcMock.submitTasksRequests).hasSize(1);
    assertThat(tasksGrpcMock.submitTasksRequests.get(0).getTaskCreationsCount()).isEqualTo(2);
    assertThat(third.deferredTaskInfo().toCompletableFuture()).isNotDone();

    // When
    scheduler.tick(100, MILLISECONDS);
    third.deferredTaskInfo().toCompletableFuture().join();

    // Then
    assertThat(tasksGrpcMock.submitTasksRequests).hasSize(2);
  }

  @Test
  @DisplayName("should send each task immediately when batching is disabled")
  void should_send_each_task_when_batching_is_disabled() {
    // Given
    var session = sessionHandle(TaskSubmissionBatching.DISABLED);

    // When
    session.submitTask(task()).deferredTaskInfo().toCompletableFuture().join();
    session.submitTask(task()).deferredTaskInfo().toCompletableFuture().join();

    // Then
    assertThat(tasksGrpcMock.submitTasksRequests).hasSize(2);
    assertThat(tasksGrpcMock.submitTasksRequests).allSatisfy(request -> assertThat(request.getTaskCreationsCount()).isEqualTo(1));
  }

  @Test
  @DisplayName("should send buffered tasks with an explicit list without waiting for the window")
  void should_send_buffered_tasks_with_explicit_list() {
    // Given
    var session = sessionHandle(new TaskSubmissionBatching(10, Duration.ofMillis(100)));
    var buffered = session.submitTask(task());

    // When
    var listed = session.submitTasks(List.of(task(), task()));
    buffered.deferredTaskInfo().toCompletableFuture().join();
    listed.forEach(handle -> handle.deferredTaskInfo().toCompletableFuture().join());

    // Then
    assertThat(tasksGrpcMock.submitTasksRequests).hasSize(1);
    assertThat(tasksGrpcMock.submittedTasksRequest.getTaskCreationsCount()).isEqualTo(3);
  }

  @Test
  @DisplayName("should send buffered tasks on flushSubmissions and cancel the window timer")
  void should_send_buffered_tasks_on_flush() {
    // Given
    var session = sessionHandle(new TaskSubmissionBatching(10, Duration.ofMillis(100)));
    var handle = session.submitTask(task());

    // When
    session.flushSubmissions().toCompletableFuture().join();
    scheduler.tick(100, MILLISECONDS);

    // Then
    assertThat(handle.deferredTaskInfo().toCompletableFuture()).isCompleted();
    assertThat(tasksGrpcMock.submitTasksRequests).hasSize(1);
    assertThat(scheduler.isIdle()).isTrue();
  }

  @Test
  @DisplayName("should send buffered tasks before closing the session")
  void should_send_buffered_tasks_before_close() {
    // Given
    var session = sessionHandle(new TaskSubmissionBatching(10, Duration.ofMillis(100)));
    var handle = session.submitTask(task());

    // When
    session.close().toCompletableFuture().join();

    // Then
    assertThat(handle.deferredTaskInfo().toCompletableFuture()).isCompleted();
    assertThat(tasksGrpcMock.submitTasksRequests).hasSize(1);
    assertThat(sessionsGrpcMock.submittedCloseSessionRequest).isNotNull();
  }

  @Test
  @DisplayName("should reject invalid batching settings")
  void should_reject_invalid_batching_settings() {
    assertThatThrownBy(() -> new TaskSubmissionBatching(0, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new TaskSubmissionBatching(1, Duration.ofMillis(-1))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new TaskSubmissionBatching(1, null)).isInstanceOf(NullPointerException.class);
  }

  private SessionHandle sessionHandle(TaskSubmissionBatching batching) {
    var taskSubmitter = new TaskSubmitter(sessionInfo.id(), sessionInfo.taskConfiguration(), null, channelPool, batching, scheduler);
    return new SessionHandle(sessionInfo, channelPool, taskSubmitter);
  }

  private static TaskDefinition task() {
    return new TaskDefinition().withInput("data", blobHandle("sessionId", "data-id")).withOutput("result");
  }
}
