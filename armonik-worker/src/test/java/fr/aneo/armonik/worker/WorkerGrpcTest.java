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
import static fr.aneo.armonik.worker.TaskOutcome.SUCCESS;
import static fr.aneo.armonik.worker.TaskOutcome.Success;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
class WorkerGrpcTest {
  private TaskProcessor taskProcessor;
  private WorkerGrpc worker;
  private TaskContext taskContext;

  @BeforeEach
  void setUp() {
    taskProcessor = mock(TaskProcessor.class);
    var agentStub = mock(AgentFutureStub.class, RETURNS_DEEP_STUBS);
    taskContext = mock(TaskContext.class);
    worker = new WorkerGrpc(agentStub, taskProcessor, (a, r) -> taskContext);
  }

  @SuppressWarnings("unchecked")
  @DisplayName("Should send an OK when the TaskProcessor returns Success, then complete the stream")
  @Test
  void should_send_ok_on_success_then_complete() {
    // given
    when(taskProcessor.processTask(any())).thenReturn(new Success());
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

  @Test
  @DisplayName("should report NOT_SERVING while process() is running")
  void should_report_not_serving_while_process_is_running() throws Exception {
    // Given
    var processObserver = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();
    var processingStarted = new CountDownLatch(1);
    var allowCompletion = new CountDownLatch(1);
    when(taskProcessor.processTask(taskContext)).thenAnswer(invocation -> {
      processingStarted.countDown();
      allowCompletion.await(2, SECONDS);
      return SUCCESS;
    });
    var processingThread = new Thread(() -> worker.process(request, processObserver));
    processingThread.start();
    processingStarted.await(1, SECONDS);

    // When
    var healthCheckObserver = (StreamObserver<HealthCheckReply>) mock(StreamObserver.class);
    worker.healthCheck(Empty.getDefaultInstance(), healthCheckObserver);

    // Then: Health check should report NOT_SERVING
    var replyCap = ArgumentCaptor.forClass(HealthCheckReply.class);
    var inOrder = inOrder(healthCheckObserver);
    inOrder.verify(healthCheckObserver).onNext(replyCap.capture());
    inOrder.verify(healthCheckObserver).onCompleted();
    inOrder.verifyNoMoreInteractions();

    assertThat(replyCap.getValue().getStatus()).isEqualTo(NOT_SERVING);

    // Cleanup
    allowCompletion.countDown();
    processingThread.join(2000);
  }

  @Test
  @DisplayName("should report SERVING after process() completes")
  void should_report_serving_after_process_completes() {
    // Given: Normal task processing
    when(taskProcessor.processTask(taskContext)).thenReturn(SUCCESS);
    var processObserver = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();
    worker.process(request, processObserver);

    // When
    var healthCheckObserver = (StreamObserver<HealthCheckReply>) mock(StreamObserver.class);
    worker.healthCheck(Empty.getDefaultInstance(), healthCheckObserver);


    // Then: Health check should report SERVING
    var replyCap = ArgumentCaptor.forClass(HealthCheckReply.class);
    var inOrder = inOrder(healthCheckObserver);
    inOrder.verify(healthCheckObserver).onNext(replyCap.capture());
    inOrder.verify(healthCheckObserver).onCompleted();
    inOrder.verifyNoMoreInteractions();

    assertThat(replyCap.getValue().getStatus()).isEqualTo(SERVING);
  }

  @Test
  @DisplayName("should call awaitCompletion after processTask returns success")
  void should_call_await_completion_after_process_task_returns_success() {
    // Given
    when(taskProcessor.processTask(taskContext)).thenReturn(SUCCESS);
    @SuppressWarnings("unchecked")
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // When
    worker.process(request, observer);

    // Then:
    var inOrder = inOrder(taskProcessor, taskContext, observer);
    inOrder.verify(taskProcessor).processTask(taskContext);
    inOrder.verify(taskContext).awaitCompletion();
    inOrder.verify(observer).onNext(any(ProcessReply.class));
    inOrder.verify(observer).onCompleted();
  }

  @Test
  @DisplayName("should convert awaitCompletion exception to error response")
  void should_convert_await_completion_exception_to_error() {
    // Given
    when(taskProcessor.processTask(taskContext)).thenReturn(SUCCESS);
    doThrow(new ArmoniKException("Boom !!")).when(taskContext).awaitCompletion();

    @SuppressWarnings("unchecked")
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // When
    worker.process(request, observer);

    // Then:
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    var inOrder = inOrder(observer);
    inOrder.verify(observer).onNext(replyCaptor.capture());
    inOrder.verify(observer).onCompleted();
    inOrder.verifyNoMoreInteractions();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasOk()).isFalse();
    assertThat(reply.getOutput().hasError()).isTrue();
  }


  @Test
  @DisplayName("should remain NOT_SERVING during awaitCompletion")
  void should_remain_not_serving_during_await_completion() throws Exception {
    // Given
    var awaitCompletionStarted = new CountDownLatch(1);
    var allowAwaitCompletion = new CountDownLatch(1);
    when(taskProcessor.processTask(taskContext)).thenReturn(SUCCESS);
    doAnswer(invocation -> {
      awaitCompletionStarted.countDown();
      allowAwaitCompletion.await(2, SECONDS);
      return null;
    }).when(taskContext).awaitCompletion();

    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // When
    var processingThread = new Thread(() -> worker.process(request, observer));
    processingThread.start();

    // Wait for awaitCompletion to be called
    awaitCompletionStarted.await(1, SECONDS);

    // Then: Should still be NOT_SERVING during awaitCompletion
    assertThat(worker.servingStatus.get()).isEqualTo(NOT_SERVING);

    // When: Allow awaitCompletion to finish
    allowAwaitCompletion.countDown();
    processingThread.join(2000);

    // Then: Should now be SERVING
    assertThat(worker.servingStatus.get()).isEqualTo(SERVING);
  }

  @Test
  @DisplayName("should NOT call awaitCompletion when processTask throws exception")
  void should_not_call_await_completion_when_process_task_throws() {
    // Given
    when(taskProcessor.processTask(taskContext)).thenThrow(new RuntimeException("Task processing failed"));
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // When
    worker.process(request, observer);

    // Then: awaitCompletion should NOT be called
    verify(taskContext, never()).awaitCompletion();

    // And: Should still send error response
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    verify(observer).onNext(replyCaptor.capture());
    verify(observer).onCompleted();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasError()).isTrue();
  }
}
