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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
    blobCompletionEventWatcher = new BlobCompletionEventWatcher(channel);
  }


  @Test
  void should_call_observer_on_success_when_blob_update_status_is_completed() {
    // Given
    var observerMock = new BlobCompletionListenerMock();
    resultsGrpcMock.setDownloadContentFor(blobId("BlobId"), "Hello World !!".getBytes(UTF_8));

    // when
    var done = blobCompletionEventWatcher.watch(sessionId, List.of(blobHandle), observerMock);
    eventsGrpcMock.emitStatusUpdate(blobInfo.id(), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(observerMock.blobErrors).isEmpty();
    assertThat(observerMock.blobs).hasSize(1);
    Assertions.assertThat(observerMock.blobs.get(0).data()).isEqualTo("Hello World !!".getBytes(UTF_8));
    Assertions.assertThat(observerMock.blobs.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_call_observer_on_error_when_blob_update_status_is_aborted() {
    // Given
    var observerMock = new BlobCompletionListenerMock();


    // when
    var done = blobCompletionEventWatcher.watch(sessionId, List.of(blobHandle), observerMock);
    eventsGrpcMock.emitStatusUpdate(blobInfo.id(), RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(observerMock.blobs).isEmpty();
    assertThat(observerMock.blobErrors).hasSize(1);
    Assertions.assertThat(observerMock.blobErrors.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_call_observer_on_success_when_new_blob_status_is_completed() {
    // Given
    var observerMock = new BlobCompletionListenerMock();
    resultsGrpcMock.setDownloadContentFor(blobInfo.id(), "Hello World !!".getBytes(UTF_8));

    // when
    var done = blobCompletionEventWatcher.watch(sessionId, List.of(blobHandle), observerMock);
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(observerMock.blobErrors).isEmpty();
    assertThat(observerMock.blobs).hasSize(1);
    Assertions.assertThat(observerMock.blobs.get(0).data()).isEqualTo("Hello World !!".getBytes(UTF_8));
    Assertions.assertThat(observerMock.blobs.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_call_observer_on_error_when_new_blob_update_status_is_aborted() {
    // Given
    var observerMock = new BlobCompletionListenerMock();

    // when
    var done = blobCompletionEventWatcher.watch(sessionId, List.of(blobHandle), observerMock);
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(observerMock.blobs).isEmpty();
    assertThat(observerMock.blobErrors).hasSize(1);
    Assertions.assertThat(observerMock.blobErrors.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_call_observer_on_error_when_downloading_blob_failed() {
    // Given
    var completionListenerMock = new BlobCompletionListenerMock();
    resultsGrpcMock.failDownloadFor(blobInfo.id());

    // when
    var done = blobCompletionEventWatcher.watch(sessionId, List.of(blobHandle), completionListenerMock);
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(completionListenerMock.blobs).isEmpty();
    assertThat(completionListenerMock.blobErrors).hasSize(1);
    Assertions.assertThat(completionListenerMock.blobErrors.get(0).blobInfo().id()).isEqualTo(blobInfo.id());
  }

  @Test
  void should_complete_even_if_blob_listener_throws_on_success() {
    // Given
    var failingListenerMock = new FailingListenerMock();
    resultsGrpcMock.setDownloadContentFor(blobInfo.id(), "OK".getBytes(UTF_8));

    // When
    var done = blobCompletionEventWatcher.watch(sessionId, List.of(blobHandle), failingListenerMock);
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();

    // Then
    assertThatCode(() -> done.toCompletableFuture().get(2, SECONDS)).doesNotThrowAnyException();
    assertThat(done.toCompletableFuture()).isDone();
    assertThat(done.toCompletableFuture()).isNotCompletedExceptionally();
    assertThat(failingListenerMock.successCalls).isEqualTo(1);
    assertThat(failingListenerMock.errorCalls).isEqualTo(0);
  }

  @Test
  void should_complete_even_if_blob_listener_throws_on_error() {
    // Given
    var failingListenerMock = new FailingListenerMock();

    // When
    var done = blobCompletionEventWatcher.watch(sessionId, List.of(blobHandle), failingListenerMock);
    eventsGrpcMock.emitNewResult(blobInfo.id(), RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();

    // Then
    assertThatCode(() -> done.toCompletableFuture().get(2, SECONDS)).doesNotThrowAnyException();
    assertThat(done.toCompletableFuture()).isDone();
    assertThat(done.toCompletableFuture()).isNotCompletedExceptionally();
    assertThat(failingListenerMock.successCalls).isEqualTo(0);
    assertThat(failingListenerMock.errorCalls).isEqualTo(1);
  }

  @Override
  protected List<BindableService> services() {
    return List.of(eventsGrpcMock, resultsGrpcMock);
  }

  static class BlobCompletionListenerMock implements BlobCompletionListener {
    List<Blob> blobs = new ArrayList<>();
    List<BlobError> blobErrors = new ArrayList<>();

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
    int successCalls = 0;
    int errorCalls = 0;

    @Override
    public void onSuccess(Blob blob) {
      successCalls++;
      throw new RuntimeException("boom in success");
    }

    @Override
    public void onError(BlobError blobError) {
      errorCalls++;
      throw new RuntimeException("boom in failure");
    }
  }
}
