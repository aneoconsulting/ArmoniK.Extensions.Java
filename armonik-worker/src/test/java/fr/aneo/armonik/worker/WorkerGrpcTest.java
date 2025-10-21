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
package fr.aneo.armonik.worker;

import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;

import static fr.aneo.armonik.api.grpc.v1.Objects.Empty;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus.NOT_SERVING;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus.SERVING;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessReply;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@DisplayName("WorkerGrpc — process & health status")
class WorkerGrpcTest {
  private TaskProcessor taskProcessor;
  private WorkerGrpc worker;

  @BeforeEach
  void setUp() {
    var agentStub = mock(AgentFutureStub.class, RETURNS_DEEP_STUBS);
    taskProcessor = mock(TaskProcessor.class);
    worker = new WorkerGrpc(agentStub, taskProcessor, (a, r) -> mock(TaskContext.class));
  }

  @SuppressWarnings("unchecked")
  @DisplayName("Should send an OK when the TaskProcessor returns Success, then complete the stream")
  @Test
  void should_send_ok_on_success_then_complete() {
    // given
    when(taskProcessor.processTask(any())).thenReturn(new TaskOutcome.Success());
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    worker.process(request, observer);

    // then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    var inOrder = inOrder(observer);
    inOrder.verify(observer).onNext(replyCaptor.capture());
    inOrder.verify(observer).onCompleted();
    inOrder.verifyNoMoreInteractions();

    var reply = replyCaptor.getValue();
    assertThat(reply).isNotNull();
    assertThat(reply.getOutput().hasOk()).isTrue();
    assertThat(reply.getOutput().hasError()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @DisplayName("Should send an Error when the TaskProcessor returns a domain error, then complete the stream")
  @Test
  void should_send_error_when_TaskProcessor_returns_a_domain_error_then_complete() {
    // given
    when(taskProcessor.processTask(any())).thenReturn(new TaskOutcome.Error("boom"));
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    worker.process(request, observer);

    // then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    var inOrder = inOrder(observer);
    inOrder.verify(observer).onNext(replyCaptor.capture());
    inOrder.verify(observer).onCompleted();
    inOrder.verifyNoMoreInteractions();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasOk()).isFalse();
    assertThat(reply.getOutput().hasError()).isTrue();
    assertThat(reply.getOutput().getError().getDetails()).isEqualTo("boom");
  }

  @SuppressWarnings("unchecked")
  @DisplayName("Should map unexpected exceptions thrown by the TaskProcessor to an Error output and complete the stream")
  @Test
  void should_map_exception_to_error_then_complete() {
    // given
    when(taskProcessor.processTask(any())).thenThrow(new RuntimeException("kaboom"));
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    worker.process(request, observer);

    // then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    var inOrder = inOrder(observer);
    inOrder.verify(observer).onNext(replyCaptor.capture());
    inOrder.verify(observer).onCompleted();
    inOrder.verifyNoMoreInteractions();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasOk()).isFalse();
    assertThat(reply.getOutput().hasError()).isTrue();
    assertThat(reply.getOutput().getError().getDetails()).contains("kaboom");
  }

  @SuppressWarnings("unchecked")
  @DisplayName("Should report SERVING when idle (no task in progress)")
  @Test
  void should_report_serving_when_idle() {
    // given
    var observer = (StreamObserver<HealthCheckReply>) mock(StreamObserver.class);

    // when
    worker.healthCheck(Empty.getDefaultInstance(), observer);

    // then
    var replyCap = ArgumentCaptor.forClass(HealthCheckReply.class);
    var inOrder = inOrder(observer);
    inOrder.verify(observer).onNext(replyCap.capture());
    inOrder.verify(observer).onCompleted();
    inOrder.verifyNoMoreInteractions();

    assertThat(replyCap.getValue().getStatus()).isEqualTo(SERVING);
  }

  @DisplayName("Should report NOT_SERVING while process() is running")
  @Test
  void should_report_not_serving_while_process_is_running() throws Exception {
    {
      // given
      var scenario = new SingleTaskProcessingScenario();

      // when
      scenario.startProcessingAsync();
      scenario.awaitProcessingStarted();

      // then
      var duringProcessingReply = scenario.healthCheck();
      assertThat(duringProcessingReply.getStatus()).isEqualTo(NOT_SERVING);

      // cleanup
      scenario.completeProcessingAndWait();
    }
  }

  @DisplayName("Should report SERVING after process() completes")
  @Test
  void should_report_serving_after_process_completes() throws Exception {
    {
      // given
      var scenario = new SingleTaskProcessingScenario();

      // when
      scenario.startProcessingAsync();
      scenario.awaitProcessingStarted();
      scenario.completeProcessingAndWait();

      // then
      var afterCompletionReply = scenario.healthCheck();
      assertThat(afterCompletionReply.getStatus()).isEqualTo(SERVING);
    }
  }


  @SuppressWarnings("unchecked")
  private static final class SingleTaskProcessingScenario {
    final CountDownLatch processingStarted = new CountDownLatch(1);
    final CountDownLatch allowCompletion = new CountDownLatch(1);
    final CountDownLatch processingCompleted = new CountDownLatch(1);
    final AgentFutureStub agentStub;
    final WorkerGrpc workerUnderTest;
    final ProcessRequest request = ProcessRequest.newBuilder().build();
    final StreamObserver<ProcessReply> processObserver = mock(StreamObserver.class);

    SingleTaskProcessingScenario() {
      this.agentStub = mock(AgentFutureStub.class, RETURNS_DEEP_STUBS);
      TaskContextFactory taskContextFactory = (a, r) -> mock(TaskContext.class);
      TaskProcessor taskProcessor = context -> {
        processingStarted.countDown();
        try {
          allowCompletion.await(2, SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return new TaskOutcome.Success();
      };

      this.workerUnderTest = new WorkerGrpc(agentStub, taskProcessor, taskContextFactory);
    }

    void startProcessingAsync() {
      new Thread(() -> {
        try {
          workerUnderTest.process(request, processObserver);
        } finally {
          processingCompleted.countDown();
        }
      }).start();
    }

    void awaitProcessingStarted() throws InterruptedException {
      processingStarted.await(1, SECONDS);
    }

    void completeProcessingAndWait() throws InterruptedException {
      allowCompletion.countDown();
      processingCompleted.await(1, SECONDS);
    }

    HealthCheckReply healthCheck() {
      var healthCheckObserver = (StreamObserver<HealthCheckReply>) mock(StreamObserver.class);
      var cap = ArgumentCaptor.forClass(HealthCheckReply.class);
      workerUnderTest.healthCheck(Empty.getDefaultInstance(), healthCheckObserver);
      var order = inOrder(healthCheckObserver);
      order.verify(healthCheckObserver).onNext(cap.capture());
      order.verify(healthCheckObserver).onCompleted();
      order.verifyNoMoreInteractions();
      return cap.getValue();
    }
  }
}
