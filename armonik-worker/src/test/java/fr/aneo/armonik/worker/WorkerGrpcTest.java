package fr.aneo.armonik.worker;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessReply;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class WorkerGrpcTest {
  private TaskProcessor taskProcessor;
  private WorkerGrpc worker;

  @BeforeEach
  void setUp() {
    var agentStub = mock(AgentFutureStub.class, RETURNS_DEEP_STUBS);
    taskProcessor = mock(TaskProcessor.class);
    worker = new WorkerGrpc(agentStub, taskProcessor);
  }

  @SuppressWarnings("unchecked")
  @DisplayName("Should send an OK when the TaskProcessor returns Success, then complete the stream")
  @Test
  void should_send_ok_on_success_then_complete() {
    // given
    when(taskProcessor.processTask(any())).thenReturn(new TaskOutcome.Success());
    StreamObserver<ProcessReply> observer = mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    worker.process(request, observer);

    // then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    InOrder inOrder = inOrder(observer);
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
    StreamObserver<ProcessReply> observer = mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    worker.process(request, observer);

    // then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    InOrder inOrder = inOrder(observer);
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
    StreamObserver<ProcessReply> observer = mock(StreamObserver.class);
    var request = ProcessRequest.newBuilder().build();

    // when
    worker.process(request, observer);

    // then
    var replyCaptor = ArgumentCaptor.forClass(ProcessReply.class);
    InOrder inOrder = inOrder(observer);
    inOrder.verify(observer).onNext(replyCaptor.capture());
    inOrder.verify(observer).onCompleted();
    inOrder.verifyNoMoreInteractions();

    var reply = replyCaptor.getValue();
    assertThat(reply.getOutput().hasOk()).isFalse();
    assertThat(reply.getOutput().hasError()).isTrue();
    assertThat(reply.getOutput().getError().getDetails()).contains("kaboom");
  }
}
