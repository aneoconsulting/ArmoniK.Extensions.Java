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

import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.ResultsGrpcMock;
import io.grpc.BindableService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_ABORTED;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_COMPLETED;
import static fr.aneo.armonik.client.TestDataFactory.blobInfo;
import static fr.aneo.armonik.client.TestDataFactory.sessionId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class BlobCompletionPollerTest extends InProcessGrpcTestBase {
  private static final Duration MIN = Duration.ofMillis(10);
  private static final Duration MAX = Duration.ofMillis(80);

  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private ScheduledExecutorService scheduler;
  private SessionId sessionId;
  private RecordingListener listener;

  @Override
  protected List<BindableService> services() {
    return List.of(resultsGrpcMock);
  }

  @BeforeEach
  void setUp() {
    resultsGrpcMock.reset();
    scheduler = Executors.newSingleThreadScheduledExecutor();
    sessionId = sessionId();
    listener = new RecordingListener();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  @DisplayName("should download a completed output and hand it to the listener")
  @Timeout(5)
  void should_download_completed_output() throws Exception {
    // Given
    var info = blobInfo("out");
    resultsGrpcMock.setDownloadContentFor(info.id(), "Hello".getBytes(UTF_8));
    var poller = poller(listener, true, Runnable::run);

    // When
    poller.enqueue(List.of(handle(info)));
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    poller.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.blobs).hasSize(1);
    assertThat(new String(listener.blobs.get(0).data(), UTF_8)).isEqualTo("Hello");
    assertThat(listener.errors).isEmpty();
  }

  @Test
  @DisplayName("should report an aborted output as failed")
  @Timeout(5)
  void should_report_aborted_output() throws Exception {
    // Given
    var info = blobInfo("out");
    var poller = poller(listener, true, Runnable::run);

    // When
    poller.enqueue(List.of(handle(info)));
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_ABORTED);
    poller.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.blobs).isEmpty();
    assertThat(listener.errors).hasSize(1);
    assertThat(resultsGrpcMock.downloadCalls.get()).isZero();
  }

  @Test
  @DisplayName("should report a failed download as an error")
  @Timeout(5)
  void should_report_failed_download() throws Exception {
    // Given
    var info = blobInfo("out");
    resultsGrpcMock.failDownloadFor(info.id());
    var poller = poller(listener, true, Runnable::run);

    // When
    poller.enqueue(List.of(handle(info)));
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    poller.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.blobs).isEmpty();
    assertThat(listener.errors).hasSize(1);
  }

  @Test
  @DisplayName("should become idle even when the listener throws")
  @Timeout(5)
  void should_become_idle_when_listener_throws() throws Exception {
    // Given
    var info = blobInfo("out");
    var throwing = new BlobCompletionListener() {
      @Override
      public void onSuccess(Blob blob) {
        throw new IllegalStateException("listener failure");
      }

      @Override
      public void onError(BlobError blobError) {
        throw new IllegalStateException("listener failure");
      }
    };
    var poller = poller(throwing, true, Runnable::run);

    // When
    poller.enqueue(List.of(handle(info)));
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);

    // Then
    poller.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
  }

  @Test
  @DisplayName("should not download outputs when nobody reads them")
  @Timeout(5)
  void should_not_download_without_listener() throws Exception {
    // Given
    var info = blobInfo("out");
    var poller = poller(listener, false, Runnable::run);

    // When
    poller.enqueue(List.of(handle(info)));
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    poller.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(resultsGrpcMock.downloadCalls.get()).isZero();
    assertThat(listener.blobs).isEmpty();
  }

  @Test
  @DisplayName("should call the listener on the listener executor and wait for it")
  @Timeout(5)
  void should_call_listener_on_listener_executor() throws Exception {
    // Given: an executor that only queues the listener calls
    var queued = new LinkedBlockingQueue<Runnable>();
    var info = blobInfo("out");
    var poller = poller(listener, true, queued::add);

    // When
    poller.enqueue(List.of(handle(info)));
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    var call = queued.poll(3, SECONDS);

    // Then: the output is not processed until the call runs
    assertThat(call).isNotNull();
    var idle = poller.waitUntilIdle().toCompletableFuture();
    assertThat(idle).isNotDone();
    call.run();
    idle.get(3, SECONDS);
    assertThat(listener.blobs).hasSize(1);
  }

  @Test
  @DisplayName("should keep delivering outputs while listener calls block as many threads as the client has")
  @Timeout(10)
  void should_keep_delivering_while_listener_calls_block() throws Exception {
    // Given: one more output than the client has threads; every listener call but the last blocks
    var blocked = Runtime.getRuntime().availableProcessors();
    var infos = IntStream.rangeClosed(0, blocked).mapToObj(i -> blobInfo("out-" + i)).toList();
    var lastId = infos.get(blocked).id().asString();
    var release = new CountDownLatch(1);
    var blocking = new RecordingListener() {
      @Override
      public void onSuccess(Blob blob) {
        super.onSuccess(blob);
        if (!blob.blobInfo().id().asString().equals(lastId)) await(release);
      }
    };
    var listenerExecutor = Executors.newFixedThreadPool(blocked + 1);
    try {
      var poller = poller(blocking, true, listenerExecutor);

      // When
      poller.enqueue(infos.stream().map(this::handle).toList());
      infos.forEach(info -> resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED));

      // Then: the last output is delivered while the others block
      awaitCondition(() -> blocking.blobs.stream().anyMatch(blob -> blob.blobInfo().id().asString().equals(lastId)));
      assertThat(poller.waitUntilIdle().toCompletableFuture()).isNotDone();
      release.countDown();
      poller.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
    } finally {
      release.countDown();
      listenerExecutor.shutdownNow();
    }
  }

  @Test
  @DisplayName("should poll less often while no output progresses, and stop once none is pending")
  @Timeout(5)
  void should_back_off_and_stop_polling() throws Exception {
    // Given: an output that stays CREATED for one second
    var info = blobInfo("out");
    var poller = poller(listener, false, Runnable::run);
    poller.enqueue(List.of(handle(info)));

    // When
    Thread.sleep(1000);
    var callsWhileWaiting = resultsGrpcMock.listResultsCalls.get();
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    poller.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
    var callsAtIdle = resultsGrpcMock.listResultsCalls.get();
    Thread.sleep(300);

    // Then: about one call per 80 ms once backed off, instead of one per 10 ms, and none once idle
    assertThat(callsWhileWaiting).isBetween(2, 25);
    assertThat(resultsGrpcMock.listResultsCalls.get()).isEqualTo(callsAtIdle);
  }

  @Test
  @DisplayName("should keep polling after a failed listing")
  @Timeout(5)
  void should_retry_after_failed_listing() throws Exception {
    // Given
    resultsGrpcMock.failingListResults.set(2);
    var info = blobInfo("out");
    var poller = poller(listener, true, Runnable::run);

    // When
    poller.enqueue(List.of(handle(info)));
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    poller.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.blobs).hasSize(1);
  }

  @Test
  @DisplayName("should list more outputs than one request holds")
  @Timeout(10)
  void should_list_more_outputs_than_one_request_holds() throws Exception {
    // Given
    var infos = IntStream.range(0, BlobCompletionPoller.MAX_IDS_PER_REQUEST + 500).mapToObj(i -> blobInfo("out-" + i)).toList();
    var poller = poller(listener, false, Runnable::run);

    // When
    poller.enqueue(infos.stream().map(this::handle).toList());
    infos.forEach(info -> resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED));

    // Then
    poller.waitUntilIdle().toCompletableFuture().get(5, SECONDS);
  }

  @Test
  @DisplayName("should be idle at once with nothing to watch")
  void should_be_idle_with_nothing_to_watch() {
    var poller = poller(listener, true, Runnable::run);
    poller.enqueue(List.of());
    assertThat(poller.waitUntilIdle().toCompletableFuture()).isDone();
    assertThat(resultsGrpcMock.listResultsCalls.get()).isZero();
  }

  @Test
  @DisplayName("should stop polling and release waiters once the pool is closed")
  @Timeout(5)
  void should_stop_once_pool_is_closed() throws Exception {
    // Given: an output that never completes, watched through a pool that gets closed
    var pool = channelPool;
    var poller = poller(listener, false, Runnable::run);
    poller.enqueue(List.of(handle(blobInfo("out"))));
    var idle = poller.waitUntilIdle().toCompletableFuture();

    // When
    pool.close();

    // Then
    idle.get(3, SECONDS);
    var calls = resultsGrpcMock.listResultsCalls.get();
    Thread.sleep(300);
    assertThat(resultsGrpcMock.listResultsCalls.get()).isEqualTo(calls);
  }

  private BlobCompletionPoller poller(BlobCompletionListener listener, boolean downloadData, Executor listenerExecutor) {
    return new BlobCompletionPoller(sessionId, channelPool, listener, downloadData, listenerExecutor, scheduler, MIN, MAX);
  }

  private BlobHandle handle(BlobInfo info) {
    return new BlobHandle(sessionId, completedFuture(info), channelPool);
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
    var deadline = System.nanoTime() + SECONDS.toNanos(3);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
    assertThat(condition.getAsBoolean()).isTrue();
  }

  static class RecordingListener implements BlobCompletionListener {
    final List<Blob> blobs = new CopyOnWriteArrayList<>();
    final List<BlobError> errors = new CopyOnWriteArrayList<>();

    @Override
    public void onSuccess(Blob blob) {
      blobs.add(blob);
    }

    @Override
    public void onError(BlobError blobError) {
      errors.add(blobError);
    }
  }
}
