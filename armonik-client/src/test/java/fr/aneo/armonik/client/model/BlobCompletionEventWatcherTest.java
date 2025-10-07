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
package fr.aneo.armonik.client.model;

import fr.aneo.armonik.client.testutils.EventsGrpcMock;
import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.ResultsGrpcMock;
import io.grpc.BindableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_ABORTED;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_COMPLETED;
import static fr.aneo.armonik.client.model.TestDataFactory.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class BlobCompletionEventWatcherTest extends InProcessGrpcTestBase {

  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private final EventsGrpcMock eventsGrpcMock = new EventsGrpcMock();
  private BlobHandle blobHandle;
  private SessionId sessionId;
  private BlobInfo blobInfo;
  private BlobCompletionEventWatcher blobCompletionEventWatcher;

  @BeforeEach
  void setUp() {
    sessionId = sessionId();
    blobInfo = blobInfo("BlobId");
    blobHandle = new BlobHandle(sessionId, completedFuture(blobInfo), channel);
    resultsGrpcMock.reset();
  }


  @Test
  void should_call_observer_on_success_when_blob_update_status_is_completed() {
    // Given
    var listenerMock = new BlobCompletionListenerMock();
    blobCompletionEventWatcher = new BlobCompletionEventWatcher(sessionId, channel, listenerMock);
    resultsGrpcMock.setDownloadContentFor(blobId("BlobId"), "Hello World !!".getBytes(UTF_8));

    // when
    var done = blobCompletionEventWatcher.watch(List.of(blobHandle));
    eventsGrpcMock.emitStatusUpdate(blobInfo.id(), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.completion().toCompletableFuture().join();

    // then
    assertThat(listenerMock.blobErrors).isEmpty();
    assertThat(listenerMock.blobs).hasSize(1);
    assertThat(listenerMock.blobs.get(0).data()).isEqualTo("Hello World !!".getBytes(UTF_8));
    assertThat(listenerMock.blobs.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_call_observer_on_error_when_blob_update_status_is_aborted() {
    // Given
    var listenerMock = new BlobCompletionListenerMock();
    blobCompletionEventWatcher = new BlobCompletionEventWatcher(sessionId, channel, listenerMock);


    // when
    var done = blobCompletionEventWatcher.watch(List.of(blobHandle));
    eventsGrpcMock.emitStatusUpdate(blobInfo.id(), RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();
    done.completion().toCompletableFuture().join();

    // then
    assertThat(listenerMock.blobs).isEmpty();
    assertThat(listenerMock.blobErrors).hasSize(1);
    assertThat(listenerMock.blobErrors.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_call_observer_on_success_when_new_blob_status_is_completed() {
    // Given
    var listenerMock = new BlobCompletionListenerMock();
    blobCompletionEventWatcher = new BlobCompletionEventWatcher(sessionId, channel, listenerMock);
    resultsGrpcMock.setDownloadContentFor(blobInfo.id(), "Hello World !!".getBytes(UTF_8));

    // when
    var done = blobCompletionEventWatcher.watch(List.of(blobHandle));
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.completion().toCompletableFuture().join();

    // then
    assertThat(listenerMock.blobErrors).isEmpty();
    assertThat(listenerMock.blobs).hasSize(1);
    assertThat(listenerMock.blobs.get(0).data()).isEqualTo("Hello World !!".getBytes(UTF_8));
    assertThat(listenerMock.blobs.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_call_observer_on_error_when_new_blob_update_status_is_aborted() {
    // Given
    var listenerMock = new BlobCompletionListenerMock();
    blobCompletionEventWatcher = new BlobCompletionEventWatcher(sessionId, channel, listenerMock);

    // when
    var done = blobCompletionEventWatcher.watch(List.of(blobHandle));
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();
    done.completion().toCompletableFuture().join();

    // then
    assertThat(listenerMock.blobs).isEmpty();
    assertThat(listenerMock.blobErrors).hasSize(1);
    assertThat(listenerMock.blobErrors.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_call_observer_on_error_when_downloading_blob_failed() {
    // Given
    var listenerMock = new BlobCompletionListenerMock();
    blobCompletionEventWatcher = new BlobCompletionEventWatcher(sessionId, channel, listenerMock);
    resultsGrpcMock.failDownloadFor(blobInfo.id());

    // when
    var done = blobCompletionEventWatcher.watch(List.of(blobHandle));
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.completion().toCompletableFuture().join();

    // then
    assertThat(listenerMock.blobs).isEmpty();
    assertThat(listenerMock.blobErrors).hasSize(1);
    assertThat(listenerMock.blobErrors.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_complete_even_if_blob_listener_throws_on_success() {
    // Given
    var failingListenerMock = new FailingListenerMock();
    blobCompletionEventWatcher = new BlobCompletionEventWatcher(sessionId, channel, failingListenerMock);
    resultsGrpcMock.setDownloadContentFor(blobInfo.id(), "OK".getBytes(UTF_8));

    // When
    var done = blobCompletionEventWatcher.watch(List.of(blobHandle));
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();

    // Then
    assertThatCode(() -> done.completion().toCompletableFuture().get(2, SECONDS)).doesNotThrowAnyException();
    assertThat(done.completion().toCompletableFuture()).isDone();
    assertThat(done.completion().toCompletableFuture()).isNotCompletedExceptionally();
    assertThat(failingListenerMock.successCalls.get()).isEqualTo(1);
    assertThat(failingListenerMock.errorCalls.get()).isEqualTo(0);
  }

  @Test
  void should_complete_even_if_blob_listener_throws_on_error() {
    // Given
    var failingListenerMock = new FailingListenerMock();
    blobCompletionEventWatcher = new BlobCompletionEventWatcher(sessionId, channel, failingListenerMock);

    // When
    var done = blobCompletionEventWatcher.watch(List.of(blobHandle));
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();

    // Then
    assertThatCode(() -> done.completion().toCompletableFuture().get(2, SECONDS)).doesNotThrowAnyException();
    assertThat(done.completion().toCompletableFuture()).isDone();
    assertThat(done.completion().toCompletableFuture()).isNotCompletedExceptionally();
    assertThat(failingListenerMock.successCalls.get()).isEqualTo(0);
    assertThat(failingListenerMock.errorCalls.get()).isEqualTo(1);
  }

  @Test
  void should_publish_leftovers_on_completed_stream() throws Exception {
    // Given
    var sessionId = sessionId();
    var blobHandleA = blobHandle(sessionId.asString(), "A");
    var blobHandleB = blobHandle(sessionId.asString(), "B");
    var blobHandleC = blobHandle(sessionId.asString(), "C");
    var listener = new BlobCompletionListenerMock();
    var watcher = new BlobCompletionEventWatcher(sessionId, channel, listener);

    // When
    var ticket = watcher.watch(List.of(blobHandleA, blobHandleB, blobHandleC));
    eventsGrpcMock.emitStatusUpdate(blobId("A"), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.emitStatusUpdate(blobId("B"), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();

    // Then
    ticket.completion().toCompletableFuture().get(2, SECONDS);
    var leftovers = ticket.leftoversAfterCompletion().toCompletableFuture().get(2, SECONDS);
    assertThat(leftovers).containsExactly(blobHandleC);
  }

  @Test
  void should_publish_leftovers_on_error_stream() throws Exception {
    //Given
    var sessionId = sessionId();
    var blobHandleA = blobHandle(sessionId.asString(), "A");
    var blobHandleB = blobHandle(sessionId.asString(), "B");
    var listener = new BlobCompletionListenerMock();
    var watcher = new BlobCompletionEventWatcher(sessionId, channel, listener);

    // When
    var ticket = watcher.watch(List.of(blobHandleA, blobHandleB));
    eventsGrpcMock.emitStatusUpdate(blobId("A"), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.error(new RuntimeException("boom"));

    // Then
    ticket.completion().toCompletableFuture().get(2, SECONDS);
    var leftovers = ticket.leftoversAfterCompletion().toCompletableFuture().get(2, SECONDS);
    assertThat(leftovers).containsExactly(blobHandleB);
  }

  @Override
  protected List<BindableService> services() {
    return List.of(eventsGrpcMock, resultsGrpcMock);
  }

  static class BlobCompletionListenerMock implements BlobCompletionListener {
    List<Blob> blobs = new CopyOnWriteArrayList<>();
    List<BlobError> blobErrors = new CopyOnWriteArrayList<>();

    @Override
    public void onSuccess(Blob blob) {
      blobs.add(blob);
    }

    @Override
    public void onError(BlobError blobError) {
      blobErrors.add(blobError);
    }
  }

  static class FailingListenerMock implements BlobCompletionListener {
    AtomicInteger successCalls = new AtomicInteger();
    AtomicInteger errorCalls = new  AtomicInteger();

    @Override
    public void onSuccess(Blob blob) {
      successCalls.incrementAndGet();
      throw new RuntimeException("boom in success");
    }

    @Override
    public void onError(BlobError blobError) {
      errorCalls.incrementAndGet();
      throw new RuntimeException("boom in failure");
    }
  }
}
