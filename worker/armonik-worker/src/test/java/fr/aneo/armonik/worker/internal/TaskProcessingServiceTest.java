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
package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.api.grpc.v1.Objects.TaskOptions;
import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply;
import fr.aneo.armonik.worker.domain.ArmoniKException;
import fr.aneo.armonik.worker.domain.TaskContext;
import fr.aneo.armonik.worker.domain.TaskOutcome;
import fr.aneo.armonik.worker.domain.TaskProcessor;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

import static fr.aneo.armonik.api.grpc.v1.Objects.Empty;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus.NOT_SERVING;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus.SERVING;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessReply;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static fr.aneo.armonik.worker.domain.TaskOutcome.SUCCESS;
import static fr.aneo.armonik.worker.domain.TaskOutcome.Success;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
class TaskProcessingServiceTest {
  private TaskProcessor taskProcessor;
  private TaskProcessingService taskProcessingService;
  private TaskContext taskContext;
  private AgentFutureStub agentStub;
  private StreamObserver<ProcessReply> observer;

  @BeforeEach
  void setUp() {
    taskProcessor = mock(TaskProcessor.class);
    agentStub = mock(AgentFutureStub.class, RETURNS_DEEP_STUBS);
    taskContext = mock(TaskContext.class);
    observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
  }

  @SuppressWarnings("unchecked")
  @DisplayName("Should send an OK when the TaskProcessor returns Success, then complete the stream")
  @Test
  void should_send_ok_on_success_then_complete() {
    // given
    when(taskProcessor.processTask(any())).thenReturn(new Success());
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    taskProcessingService.process(request, observer);

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
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    taskProcessingService.process(request, observer);

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
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    taskProcessingService.process(request, observer);

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
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);

    // when
    taskProcessingService.healthCheck(Empty.getDefaultInstance(), observer);

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
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    var processingThread = new Thread(() -> taskProcessingService.process(request, processObserver));
    processingThread.start();
    processingStarted.await(1, SECONDS);

    // When
    var healthCheckObserver = (StreamObserver<HealthCheckReply>) mock(StreamObserver.class);
    taskProcessingService.healthCheck(Empty.getDefaultInstance(), healthCheckObserver);

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
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    taskProcessingService.process(request, processObserver);

    // When
    var healthCheckObserver = (StreamObserver<HealthCheckReply>) mock(StreamObserver.class);
    taskProcessingService.healthCheck(Empty.getDefaultInstance(), healthCheckObserver);


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
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    @SuppressWarnings("unchecked")
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // When
    taskProcessingService.process(request, observer);

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
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);

    @SuppressWarnings("unchecked")
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // When
    taskProcessingService.process(request, observer);

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
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // When
    var processingThread = new Thread(() -> taskProcessingService.process(request, observer));
    processingThread.start();

    // Wait for awaitCompletion to be called
    awaitCompletionStarted.await(1, SECONDS);

    // Then: Should still be NOT_SERVING during awaitCompletion
    assertThat(taskProcessingService.servingStatus.get()).isEqualTo(NOT_SERVING);

    // When: Allow awaitCompletion to finish
    allowAwaitCompletion.countDown();
    processingThread.join(2000);

    // Then: Should now be SERVING
    assertThat(taskProcessingService.servingStatus.get()).isEqualTo(SERVING);
  }

  @Test
  @DisplayName("should NOT call awaitCompletion when processTask throws exception")
  void should_not_call_await_completion_when_process_task_throws() {
    // Given
    when(taskProcessor.processTask(taskContext)).thenThrow(new RuntimeException("Task processing failed"));
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    var request = ProcessRequest.newBuilder().build();

    // When
    taskProcessingService.process(request, observer);

    // Then: awaitCompletion should NOT be called
    verify(taskContext, never()).awaitCompletion();

    // And: Should still send error response
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    verify(observer).onNext(replyCaptor.capture());
    verify(observer).onCompleted();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasError()).isTrue();
  }

  @Test
  @DisplayName("should accept task when worker has fixed taskProcessor and request has no library info")
  void should_accept_task_in_static_mode() {
    // Given
    var dynamicTaskProcessorLoader = mock(DynamicTaskProcessorLoader.class);
    when(taskProcessor.processTask(any())).thenReturn(new Success());
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext, dynamicTaskProcessorLoader);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder()
                                .setTaskOptions(TaskOptions.getDefaultInstance())
                                .build();

    // When
    taskProcessingService.process(request, observer);

    // Then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    verify(observer).onNext(replyCaptor.capture());
    verify(observer).onCompleted();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasOk()).isTrue();
    assertThat(reply.getOutput().hasError()).isFalse();
    verify(taskProcessor).processTask(taskContext);

    verifyNoInteractions(dynamicTaskProcessorLoader);
  }

  @Test
  @DisplayName("should reject task when dynamic mode worker receives request without library info")
  void should_reject_task_when_dynamic_mode_worker_lacks_library_info() {
    // Given
    taskProcessingService = new TaskProcessingService(agentStub, null, (a, r) -> taskContext);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder()
                                .setTaskOptions(TaskOptions.getDefaultInstance())
                                .build();

    // When
    taskProcessingService.process(request, observer);

    // Then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    var inOrder = inOrder(observer);
    inOrder.verify(observer).onNext(replyCaptor.capture());
    inOrder.verify(observer).onCompleted();
    inOrder.verifyNoMoreInteractions();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasOk()).isFalse();
    assertThat(reply.getOutput().hasError()).isTrue();
    assertThat(reply.getOutput().getError().getDetails())
      .contains("Worker configuration error")
      .contains("dynamic loading")
      .contains("Required task options")
      .contains("LibraryBlobId")
      .contains("LibraryPath")
      .contains("Symbol")
      .contains("ConventionVersion");

    verify(taskProcessor, never()).processTask(any());
    verify(taskContext, never()).awaitCompletion();
  }

  @Test
  @DisplayName("should reject task when static mode worker receives request with library info")
  void should_reject_task_when_static_mode_worker_receives_library_info() {
    // Given
    taskProcessingService = new TaskProcessingService(agentStub, taskProcessor, (a, r) -> taskContext);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);
    var request = createDynamicTaskProcessorRequest();

    // When
    taskProcessingService.process(request, observer);

    // Then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    var inOrder = inOrder(observer);
    inOrder.verify(observer).onNext(replyCaptor.capture());
    inOrder.verify(observer).onCompleted();
    inOrder.verifyNoMoreInteractions();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasOk()).isFalse();
    assertThat(reply.getOutput().hasError()).isTrue();
    assertThat(reply.getOutput().getError().getDetails())
      .contains("Task rejected")
      .contains("fixed taskProcessor")
      .contains("cannot load libraries dynamically")
      .contains("Remove library options");

    verify(taskProcessor, never()).processTask(any());
    verify(taskContext, never()).awaitCompletion();
  }

  @Test
  @DisplayName("should use dynamic processor when library info provided")
  void should_use_dynamic_processor_when_library_provided() {
    // Given
    var dynamicTaskProcessorLoader = mock(DynamicTaskProcessorLoader.class);
    var taskProcessor = mock(TaskProcessor.class);
    var loadedTaskProcessor = mock(LoadedTaskProcessor.class);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);

    when(loadedTaskProcessor.taskProcessor()).thenReturn(taskProcessor);
    when(dynamicTaskProcessorLoader.load(any(), any())).thenReturn(loadedTaskProcessor);
    when(taskProcessor.processTask(any())).thenReturn(TaskOutcome.SUCCESS);

    var service = new TaskProcessingService(agentStub, null, (a, r) -> taskContext, dynamicTaskProcessorLoader);
    var request = createDynamicTaskProcessorRequest();

    // When
    service.process(request, observer);

    // Then
    verify(dynamicTaskProcessorLoader).load(any(WorkerLibrary.class), eq(Path.of("/data/task-abc")));
    verify(taskProcessor).processTask(any());
    verify(loadedTaskProcessor).close();
  }

  @Test
  @DisplayName("should cleanup dynamic processor even when processing fails")
  void should_cleanup_even_on_error() {
    // Given
    var dynamicTaskProcessorLoader = mock(DynamicTaskProcessorLoader.class);
    var taskProcessor = mock(TaskProcessor.class);
    var loadedTaskProcessor = mock(LoadedTaskProcessor.class);
    var observer = (StreamObserver<ProcessReply>) mock(StreamObserver.class);

    when(loadedTaskProcessor.taskProcessor()).thenReturn(taskProcessor);
    when(dynamicTaskProcessorLoader.load(any(), any())).thenReturn(loadedTaskProcessor);
    when(taskProcessor.processTask(any())).thenThrow(new RuntimeException("Processing failed"));

    var service = new TaskProcessingService(agentStub,null,(a, r) -> taskContext,dynamicTaskProcessorLoader);

    // When
    var request = createDynamicTaskProcessorRequest();
    service.process(request, observer);

    // Then: Cleanup still called
    verify(loadedTaskProcessor).close();
  }

  private static ProcessRequest createDynamicTaskProcessorRequest() {
    var options = TaskOptions.newBuilder()
                             .putOptions(WorkerLibrary.LIBRARY_BLOB_ID, "blob-123")
                             .putOptions(WorkerLibrary.LIBRARY_PATH, "lib/processor.jar")
                             .putOptions(WorkerLibrary.SYMBOL, "com.example.Processor")
                             .putOptions(WorkerLibrary.CONVENTION_VERSION, "v1")
                             .build();
    return ProcessRequest.newBuilder()
                         .setTaskOptions(options)
                         .setDataFolder("/data/task-abc")
                         .build();
  }
}
