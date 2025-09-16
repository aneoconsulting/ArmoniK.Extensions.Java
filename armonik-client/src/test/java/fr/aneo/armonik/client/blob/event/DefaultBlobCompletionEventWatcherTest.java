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
package fr.aneo.armonik.client.blob.event;

import fr.aneo.armonik.client.blob.DefaultBlobService;
import fr.aneo.armonik.client.mocks.EventsGrpcMock;
import fr.aneo.armonik.client.mocks.ResultsGrpcMock;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.migrationsupport.rules.ExternalResourceSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_ABORTED;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_COMPLETED;
import static fr.aneo.armonik.client.blob.BlobHandleFixture.blobHandle;
import static fr.aneo.armonik.client.session.SessionHandleFixture.sessionHandle;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class DefaultBlobCompletionEventWatcherTest {

  @RegisterExtension
  public static final ExternalResourceSupport externalResourceSupport = new ExternalResourceSupport();

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private BlobCompletionEventWatcher blobCompletionEventWatcher;
  private ResultsGrpcMock resultsGrpcMock;
  private EventsGrpcMock eventsGrpcMock;

  @BeforeEach
  void setUp() throws IOException {
    resultsGrpcMock = new ResultsGrpcMock();
    eventsGrpcMock = new EventsGrpcMock();
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                                               .directExecutor()
                                               .addService(resultsGrpcMock)
                                               .addService(eventsGrpcMock)
                                               .build()
                                               .start());

    var channel = grpcCleanup.register(InProcessChannelBuilder.forName(serverName)
                                                              .directExecutor()
                                                              .build());
    blobCompletionEventWatcher = new DefaultBlobCompletionEventWatcher(channel, new DefaultBlobService(channel));
  }


  @Test
  void should_call_observer_on_success_when_blob_update_status_is_completed() {
    // Given
    var sessionHandle = sessionHandle();
    var blobHandleId = randomUUID();
    var observerMock = new BlobCompletionListenerMock();
    resultsGrpcMock.setDownloadContentFor(blobHandleId, "Hello World !!".getBytes(UTF_8));

    // when
    var done = blobCompletionEventWatcher.watch(sessionHandle, List.of(blobHandle(blobHandleId)), observerMock);
    eventsGrpcMock.emitStatusUpdate(blobHandleId, RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(observerMock.blobErrors).isEmpty();
    assertThat(observerMock.blobs).hasSize(1);
    Assertions.assertThat(observerMock.blobs.get(0).data()).isEqualTo("Hello World !!".getBytes(UTF_8));
    Assertions.assertThat(observerMock.blobs.get(0).metadata().id()).isEqualTo(blobHandleId);
  }

  @Test
  void should_call_observer_on_error_when_blob_update_status_is_aborted() {
    // Given
    var sessionHandle = sessionHandle();
    var blobHandleId = randomUUID();
    var observerMock = new BlobCompletionListenerMock();


    // when
    var done = blobCompletionEventWatcher.watch(sessionHandle, List.of(blobHandle(blobHandleId)), observerMock);
    eventsGrpcMock.emitStatusUpdate(blobHandleId, RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(observerMock.blobs).isEmpty();
    assertThat(observerMock.blobErrors).hasSize(1);
    Assertions.assertThat(observerMock.blobErrors.get(0).metadata().id()).isEqualTo(blobHandleId);
  }

  @Test
  void should_call_observer_on_success_when_new_blob_status_is_completed() {
    // Given
    var sessionHandle = sessionHandle();
    var blobHandleId = randomUUID();
    var observerMock = new BlobCompletionListenerMock();
    resultsGrpcMock.setDownloadContentFor(blobHandleId, "Hello World !!".getBytes(UTF_8));

    // when
    var done = blobCompletionEventWatcher.watch(sessionHandle, List.of(blobHandle(blobHandleId)), observerMock);
    eventsGrpcMock.emitNewResult(blobHandleId, RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(observerMock.blobErrors).isEmpty();
    assertThat(observerMock.blobs).hasSize(1);
    Assertions.assertThat(observerMock.blobs.get(0).data()).isEqualTo("Hello World !!".getBytes(UTF_8));
    Assertions.assertThat(observerMock.blobs.get(0).metadata().id()).isEqualTo(blobHandleId);
  }

  @Test
  void should_call_observer_on_error_when_new_blob_update_status_is_aborted() {
    // Given
    var sessionHandle = sessionHandle();
    var blobHandleId = randomUUID();
    var observerMock = new BlobCompletionListenerMock();

    // when
    var done = blobCompletionEventWatcher.watch(sessionHandle, List.of(blobHandle(blobHandleId)), observerMock);
    eventsGrpcMock.emitNewResult(blobHandleId, RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(observerMock.blobs).isEmpty();
    assertThat(observerMock.blobErrors).hasSize(1);
    Assertions.assertThat(observerMock.blobErrors.get(0).metadata().id()).isEqualTo(blobHandleId);
  }

  @Test
  void should_call_observer_on_error_when_downloading_blob_failed() {
    // Given
    var sessionHandle = sessionHandle();
    var blobHandleId = randomUUID();
    var completionListenerMock = new BlobCompletionListenerMock();
    resultsGrpcMock.failDownloadFor(blobHandleId);

    // when
    var done = blobCompletionEventWatcher.watch(sessionHandle, List.of(blobHandle(blobHandleId)), completionListenerMock);
    eventsGrpcMock.emitNewResult(blobHandleId, RESULT_STATUS_COMPLETED);
    eventsGrpcMock.complete();
    done.toCompletableFuture().join();

    // then
    assertThat(completionListenerMock.blobs).isEmpty();
    assertThat(completionListenerMock.blobErrors).hasSize(1);
    Assertions.assertThat(completionListenerMock.blobErrors.get(0).metadata().id()).isEqualTo(blobHandleId);
  }

  @Test
  void should_complete_even_if_blob_listener_throws_on_success() {
    // Given
    var sessionHandle = sessionHandle();
    var blobHandleId = randomUUID();
    var failingListenerMock = new FailingListenerMock();
    resultsGrpcMock.setDownloadContentFor(blobHandleId, "OK".getBytes(UTF_8));

    // When
    var done = blobCompletionEventWatcher.watch(sessionHandle, List.of(blobHandle(blobHandleId)), failingListenerMock);
    eventsGrpcMock.emitNewResult(blobHandleId, RESULT_STATUS_COMPLETED);
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
    var sessionHandle = sessionHandle();
    var blobHandleId = randomUUID();
    var failingListenerMock = new FailingListenerMock();

    // When
    var done = blobCompletionEventWatcher.watch(sessionHandle, List.of(blobHandle(blobHandleId)), failingListenerMock);
    eventsGrpcMock.emitNewResult(blobHandleId, RESULT_STATUS_ABORTED);
    eventsGrpcMock.complete();

    // Then
    assertThatCode(() -> done.toCompletableFuture().get(2, SECONDS)).doesNotThrowAnyException();
    assertThat(done.toCompletableFuture()).isDone();
    assertThat(done.toCompletableFuture()).isNotCompletedExceptionally();
    assertThat(failingListenerMock.successCalls).isEqualTo(0);
    assertThat(failingListenerMock.errorCalls).isEqualTo(1);
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
