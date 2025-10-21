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

import com.google.common.util.concurrent.SettableFuture;
import fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataRequest;
import fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataResponse;
import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AgentNotifierTest {

  private AgentFutureStub agentStub;
  private AgentNotifier notifier;
  private BlobId testBlobId;

  @BeforeEach
  void setUp() {
    agentStub = mock(AgentFutureStub.class);
    when(agentStub.withDeadlineAfter(anyLong(), eq(MILLISECONDS))).thenReturn(agentStub);
    notifier = new AgentNotifier(agentStub, "test-session", "test-token");
    testBlobId = BlobId.from("test-blob-id");
  }

  @Test
  @DisplayName("Should send notification with correct parameters")
  void should_send_notification_with_correct_parameters() {
    // Given
    when(agentStub.notifyResultData(any()))
      .thenReturn(immediateFuture(NotifyResultDataResponse.newBuilder().build()));

    // When
    notifier.onBlobReady(testBlobId);

    // Then
    ArgumentCaptor<NotifyResultDataRequest> captor = ArgumentCaptor.forClass(NotifyResultDataRequest.class);
    verify(agentStub).notifyResultData(captor.capture());

    NotifyResultDataRequest request = captor.getValue();
    assertThat(request.getCommunicationToken()).isEqualTo("test-token");
    assertThat(request.getIdsCount()).isEqualTo(1);
    assertThat(request.getIds(0).getSessionId()).isEqualTo("test-session");
    assertThat(request.getIds(0).getResultId()).isEqualTo("test-blob-id");
  }

  @Test
  @DisplayName("Should complete successfully when Agent responds immediately")
  void should_complete_successfully_when_agent_responds_immediately() {
    // Given
    when(agentStub.notifyResultData(any()))
      .thenReturn(immediateFuture(NotifyResultDataResponse.newBuilder().build()));

    // When
    notifier.onBlobReady(testBlobId);

    // Then
    verify(agentStub).notifyResultData(any());
  }

  @Test
  @DisplayName("Should block until Agent acknowledges notification")
  void should_block_until_agent_acknowledges_notification() throws Exception {
    // Given
    var enteredGet = new CountDownLatch(1);
    var delayedResponse = SettableFuture.<NotifyResultDataResponse>create();

    when(agentStub.notifyResultData(any())).thenAnswer(inv -> {
      enteredGet.countDown();
      return delayedResponse;
    });

    var thread = new Thread(() -> notifier.onBlobReady(testBlobId));

    // When
    thread.start();

    // Then
    assertThat(enteredGet.await(500, MILLISECONDS)).as("entered get()").isTrue();

    delayedResponse.set(NotifyResultDataResponse.newBuilder().build());

    thread.join(1000);
    assertThat(thread.isAlive()).isFalse();
  }

  @Test
  @DisplayName("Should throw ArmoniKException when Agent notification times out")
  void should_throw_exception_when_agent_notification_times_out() {
    // Given
    when(agentStub.notifyResultData(any()))
      .thenReturn(immediateFailedFuture(new StatusRuntimeException(Status.DEADLINE_EXCEEDED)));

    // When - Then
    assertThatThrownBy(() -> notifier.onBlobReady(testBlobId))
      .isInstanceOf(ArmoniKException.class)
      .hasMessageContaining("Agent did not acknowledge")
      .hasMessageContaining("test-blob-id")
      .hasMessageContaining("30 seconds");
  }

  @Test
  @DisplayName("Should throw ArmoniKException when Agent notification fails")
  void should_throw_exception_when_agent_notification_fails() {
    // Given
    var grpcException = new RuntimeException("gRPC connection failed");
    when(agentStub.notifyResultData(any())).thenReturn(immediateFailedFuture(grpcException));

    // When - Then
    assertThatThrownBy(() -> notifier.onBlobReady(testBlobId))
      .isInstanceOf(ArmoniKException.class)
      .hasMessageContaining("Failed to notify Agent")
      .hasMessageContaining("test-blob-id")
      .hasCauseInstanceOf(RuntimeException.class);
  }

  @Test
  @DisplayName("Should handle thread interruption during notification")
  void should_handle_thread_interruption_during_notification() throws Exception {
    // Given
    var enteredGet = new CountDownLatch(1);
    var blockingFuture = SettableFuture.<NotifyResultDataResponse>create();

    when(agentStub.notifyResultData(any())).thenAnswer(inv -> {
      enteredGet.countDown();
      return blockingFuture;
    });

    var thread = new Thread(() ->
      assertThatThrownBy(() -> notifier.onBlobReady(testBlobId))
        .isInstanceOf(ArmoniKException.class)
        .hasMessageContaining("Interrupted while notifying Agent")
        .hasMessageContaining("test-blob-id")
    );

    // When
    thread.start();

    // Then
    assertThat(enteredGet.await(500, MILLISECONDS)).as("entered get()").isTrue();

    thread.interrupt();
    thread.join(1000);

    assertThat(thread.isAlive()).isFalse();
  }
}
