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

import fr.aneo.armonik.client.testutils.EventsGrpcMock;
import fr.aneo.armonik.client.testutils.InProcessGrpcTestBase;
import fr.aneo.armonik.client.testutils.ResultsGrpcMock;
import io.grpc.BindableService;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_ABORTED;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_COMPLETED;
import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus.RESULT_STATUS_DELETED;
import static fr.aneo.armonik.client.TestDataFactory.blobInfo;
import static fr.aneo.armonik.client.TestDataFactory.sessionId;
import static fr.aneo.armonik.client.testutils.EventsGrpcMock.ids;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class BlobCompletionEventWatcherTest extends InProcessGrpcTestBase {
  private static final Duration OPEN_DELAY = Duration.ofMillis(10);
  private static final Duration REOPEN_DELAY = Duration.ofMillis(100);
  private static final Duration MIN_BACKOFF = Duration.ofMillis(20);
  private static final Duration MAX_BACKOFF = Duration.ofMillis(80);
  private static final Duration WAIT_WARNING = Duration.ofSeconds(30);

  private final ResultsGrpcMock resultsGrpcMock = new ResultsGrpcMock();
  private final EventsGrpcMock eventsGrpcMock = new EventsGrpcMock(resultsGrpcMock);
  private ScheduledExecutorService scheduler;
  private SessionId sessionId;
  private RecordingListener listener;

  @Override
  protected List<BindableService> services() {
    return List.of(resultsGrpcMock, eventsGrpcMock);
  }

  @BeforeEach
  void setUp() {
    resultsGrpcMock.reset();
    eventsGrpcMock.reset();
    scheduler = Executors.newSingleThreadScheduledExecutor();
    sessionId = sessionId();
    listener = new RecordingListener();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  @DisplayName("should download an output that completes while watched and hand it to the listener")
  @Timeout(5)
  void should_download_output_completed_while_watched() throws Exception {
    // Given
    var info = blobInfo("out");
    resultsGrpcMock.setDownloadContentFor(info.id(), "Hello".getBytes(UTF_8));
    var watcher = watcher(listener, true, Runnable::run, 1000);

    // When
    watcher.enqueue(List.of(handle(info)));
    waitFor(() -> eventsGrpcMock.openSubscriptions() == 1);
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.blobs).hasSize(1);
    assertThat(new String(listener.blobs.get(0).data(), UTF_8)).isEqualTo("Hello");
    assertThat(listener.errors).isEmpty();
  }

  @Test
  @DisplayName("should report an output that completed before its subscription opened")
  @Timeout(5)
  void should_report_output_completed_before_subscription() throws Exception {
    // Given: the output is already completed when it starts being watched
    var info = blobInfo("out");
    resultsGrpcMock.setDownloadContentFor(info.id(), "early".getBytes(UTF_8));
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    var watcher = watcher(listener, true, Runnable::run, 1000);

    // When
    watcher.enqueue(List.of(handle(info)));
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.blobs).extracting(blob -> new String(blob.data(), UTF_8)).containsExactly("early");
  }

  @Test
  @DisplayName("should report aborted and deleted outputs as failed without downloading them")
  @Timeout(5)
  void should_report_aborted_and_deleted_outputs() throws Exception {
    // Given
    var aborted = blobInfo("aborted");
    var deleted = blobInfo("deleted");
    var watcher = watcher(listener, true, Runnable::run, 1000);

    // When
    watcher.enqueue(List.of(handle(aborted), handle(deleted)));
    waitFor(() -> eventsGrpcMock.openSubscriptions() == 1);
    resultsGrpcMock.setStatus(aborted.id(), RESULT_STATUS_ABORTED);
    resultsGrpcMock.setStatus(deleted.id(), RESULT_STATUS_DELETED);
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.blobs).isEmpty();
    assertThat(listener.errors).extracting(error -> error.blobInfo().id()).containsExactlyInAnyOrder(aborted.id(), deleted.id());
    assertThat(resultsGrpcMock.downloadCalls.get()).isZero();
  }

  @Test
  @DisplayName("should report a failed download as an error")
  @Timeout(5)
  void should_report_failed_download() throws Exception {
    // Given
    var info = blobInfo("out");
    resultsGrpcMock.failDownloadFor(info.id());
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    var watcher = watcher(listener, true, Runnable::run, 1000);

    // When
    watcher.enqueue(List.of(handle(info)));
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.blobs).isEmpty();
    assertThat(listener.errors).hasSize(1);
  }

  @Test
  @DisplayName("should not download outputs when nobody reads them")
  @Timeout(5)
  void should_not_download_without_listener() throws Exception {
    // Given
    var info = blobInfo("out");
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    var watcher = watcher(listener, false, Runnable::run, 1000);

    // When
    watcher.enqueue(List.of(handle(info)));
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

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
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    var watcher = watcher(listener, true, queued::add, 1000);

    // When
    watcher.enqueue(List.of(handle(info)));
    var call = queued.poll(3, SECONDS);

    // Then: the output is not processed until the call runs
    assertThat(call).isNotNull();
    var idle = watcher.waitUntilIdle().toCompletableFuture();
    assertThat(idle).isNotDone();
    call.run();
    idle.get(3, SECONDS);
    assertThat(listener.blobs).hasSize(1);
  }

  @Test
  @DisplayName("should still notify the listener when the listener executor rejects the call")
  @Timeout(5)
  void should_notify_listener_when_executor_rejects() throws Exception {
    // Given: an executor that rejects every call, as a shut down or saturated one does
    var completed = blobInfo("completed");
    var aborted = blobInfo("aborted");
    var watcher = watcher(listener, true, task -> {
      throw new RejectedExecutionException("shut down");
    }, 1000);

    // When
    watcher.enqueue(List.of(handle(completed), handle(aborted)));
    resultsGrpcMock.setStatus(completed.id(), RESULT_STATUS_COMPLETED);
    resultsGrpcMock.setStatus(aborted.id(), RESULT_STATUS_ABORTED);

    // Then: both outputs reach the listener and the watcher becomes idle
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
    assertThat(listener.blobs).hasSize(1);
    assertThat(listener.errors).hasSize(1);
  }

  @Test
  @DisplayName("should add new outputs to the open subscription with one reopening, then cancel the old one")
  @Timeout(5)
  void should_group_new_outputs_into_one_reopening() throws Exception {
    // Given: one output watched
    var first = blobInfo("first");
    var watcher = watcher(listener, false, Runnable::run, 1000);
    watcher.enqueue(List.of(handle(first)));
    waitFor(() -> eventsGrpcMock.requests.size() == 1);

    // When: two more outputs arrive within the reopening delay
    var second = blobInfo("second");
    var third = blobInfo("third");
    watcher.enqueue(List.of(handle(second)));
    watcher.enqueue(List.of(handle(third)));
    waitFor(() -> eventsGrpcMock.requests.size() == 2 && eventsGrpcMock.cancelledSubscriptions.get() == 1);

    // Then: one subscription watches the three outputs, the first one is cancelled
    assertThat(ids(eventsGrpcMock.requests.get(1))).containsExactlyInAnyOrder(first.id().asString(), second.id().asString(), third.id().asString());
    assertThat(eventsGrpcMock.openSubscriptions()).isEqualTo(1);
    assertThat(watcher.openSubscriptions()).isEqualTo(1);

    // And: they are all reported through it
    List.of(first, second, third).forEach(info -> resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED));
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
    assertThat(eventsGrpcMock.requests).hasSize(2);
  }

  @Test
  @DisplayName("should cancel a subscription as soon as none of its outputs is pending")
  @Timeout(5)
  void should_cancel_subscription_once_nothing_is_pending() throws Exception {
    // Given
    var info = blobInfo("out");
    var watcher = watcher(listener, false, Runnable::run, 1000);
    watcher.enqueue(List.of(handle(info)));
    waitFor(() -> eventsGrpcMock.openSubscriptions() == 1);

    // When
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    waitFor(() -> eventsGrpcMock.openSubscriptions() == 0);
    assertThat(eventsGrpcMock.cancelledSubscriptions.get()).isEqualTo(1);
    assertThat(watcher.openSubscriptions()).isZero();
  }

  @Test
  @DisplayName("should cap the outputs of a subscription and reopen only the last one")
  @Timeout(5)
  void should_cap_outputs_per_subscription() throws Exception {
    // Given: at most 3 outputs per subscription, and 5 outputs
    var infos = IntStream.range(0, 5).mapToObj(i -> blobInfo("out-" + i)).toList();
    var watcher = watcher(listener, false, Runnable::run, 3);
    watcher.enqueue(infos.stream().map(this::handle).toList());
    waitFor(() -> eventsGrpcMock.openSubscriptions() == 2);
    assertThat(eventsGrpcMock.requests).extracting(request -> ids(request).size()).containsExactlyInAnyOrder(3, 2);

    // When: one more output arrives
    var late = blobInfo("late");
    watcher.enqueue(List.of(handle(late)));
    waitFor(() -> eventsGrpcMock.requests.size() == 3 && eventsGrpcMock.cancelledSubscriptions.get() == 1);

    // Then: only the subscription that was not full is reopened, with the new output
    var reopened = ids(eventsGrpcMock.requests.get(2));
    assertThat(reopened).hasSize(3).contains(late.id().asString());
    assertThat(eventsGrpcMock.openSubscriptions()).isEqualTo(2);

    // And: every output is reported
    infos.forEach(info -> resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED));
    resultsGrpcMock.setStatus(late.id(), RESULT_STATUS_COMPLETED);
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
    waitFor(() -> eventsGrpcMock.openSubscriptions() == 0);
  }

  @Test
  @DisplayName("should reopen a subscription that fails, and report what changed meanwhile")
  @Timeout(5)
  void should_reopen_failed_subscription() throws Exception {
    // Given: the first subscription fails at once, the second is cut once open
    var info = blobInfo("out");
    eventsGrpcMock.failingSubscriptions.set(1);
    var watcher = watcher(listener, false, Runnable::run, 1000);
    watcher.enqueue(List.of(handle(info)));
    waitFor(() -> eventsGrpcMock.openSubscriptions() == 1);
    eventsGrpcMock.failOpenSubscriptions(Status.UNAVAILABLE);

    // When: the output completes while no subscription is open
    resultsGrpcMock.setStatus(info.id(), RESULT_STATUS_COMPLETED);
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then: a third subscription reported it
    assertThat(eventsGrpcMock.requests).hasSize(3);
    assertThat(listener.errors).isEmpty();
  }

  @Test
  @DisplayName("should report outputs as failed when their subscription cannot succeed")
  @Timeout(5)
  void should_report_outputs_when_subscription_fails_for_good() throws Exception {
    // Given
    var info = blobInfo("out");
    eventsGrpcMock.failingSubscriptions.set(1);
    eventsGrpcMock.failingStatus = Status.PERMISSION_DENIED;
    var watcher = watcher(listener, false, Runnable::run, 1000);

    // When
    watcher.enqueue(List.of(handle(info)));
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

    // Then
    assertThat(listener.errors).hasSize(1);
    assertThat(listener.errors.get(0).cause()).hasMessageContaining("PERMISSION_DENIED");
    assertThat(eventsGrpcMock.requests).hasSize(1);
  }

  @Test
  @DisplayName("should be idle at once with nothing to watch")
  void should_be_idle_with_nothing_to_watch() {
    var watcher = watcher(listener, true, Runnable::run, 1000);

    assertThat(watcher.waitUntilIdle().toCompletableFuture()).isDone();
  }

  @Test
  @DisplayName("should report pending outputs as failed at once when the pool is closed, and outputs enqueued afterwards")
  @Timeout(5)
  void should_stop_once_pool_is_closed() throws Exception {
    // Given: an output that never completes
    var info = blobInfo("out");
    var watcher = watcher(listener, false, Runnable::run, 1000);
    watcher.enqueue(List.of(handle(info)));
    waitFor(() -> eventsGrpcMock.openSubscriptions() == 1);
    var idle = watcher.waitUntilIdle().toCompletableFuture();

    // When: the pool is closed at once, as the client closes its event pool
    var closing = System.nanoTime();
    channelPool.closeNow();

    // Then: the output is reported without waiting, and the subscription is not reopened
    idle.get(3, SECONDS);
    assertThat(System.nanoTime() - closing).isLessThan(SECONDS.toNanos(1));
    assertThat(listener.errors).hasSize(1);
    assertThat(listener.errors.get(0).cause()).hasMessageContaining("closed");
    assertThat(eventsGrpcMock.requests).hasSize(1);

    // And: an output enqueued afterwards is reported instead of awaited
    var late = blobInfo("late");
    watcher.enqueue(List.of(handle(late)));
    watcher.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
    assertThat(listener.errors).hasSize(2);
    assertThat(listener.errors.get(1).blobInfo().id()).isEqualTo(late.id());
  }

  @Test
  @DisplayName("should keep its channel when reopening, rather than lose it to a session waiting for one")
  @Timeout(10)
  void should_keep_its_channel_when_reopening() throws Exception {
    // Given: one event channel, held by session A, and session X waiting for it
    var pool = newChannelPool(1);
    try {
      var a = watcher(sessionId(), pool, listener, false, Runnable::run, 1000, MIN_BACKOFF, WAIT_WARNING);
      var x = watcher(sessionId(), pool, listener, false, Runnable::run, 1000, MIN_BACKOFF, WAIT_WARNING);
      var a1 = blobInfo("a1");
      var x1 = blobInfo("x1");
      a.enqueue(List.of(handle(a1)));
      waitFor(() -> eventsGrpcMock.openSubscriptions() == 1);
      x.enqueue(List.of(handle(x1)));
      waitFor(() -> x.openSubscriptions() == 1);

      // When: A adds an output, then both of its outputs complete
      var a2 = blobInfo("a2");
      a.enqueue(List.of(handle(a2)));
      waitFor(() -> requestsWatching(a2).size() == 1);
      resultsGrpcMock.setStatus(a1.id(), RESULT_STATUS_COMPLETED);
      resultsGrpcMock.setStatus(a2.id(), RESULT_STATUS_COMPLETED);

      // Then: A is reported on the channel it kept, and X gets the channel once A is done
      a.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
      waitFor(() -> requestsWatching(x1).size() == 1);
      resultsGrpcMock.setStatus(x1.id(), RESULT_STATUS_COMPLETED);
      x.waitUntilIdle().toCompletableFuture().get(3, SECONDS);
    } finally {
      pool.close();
    }
  }

  @Test
  @DisplayName("should add new outputs to a subscription still waiting for its channel")
  @Timeout(10)
  void should_add_outputs_to_a_subscription_waiting_for_its_channel() throws Exception {
    // Given: the only event channel is held by session X
    var pool = newChannelPool(1);
    try {
      var x = watcher(sessionId(), pool, listener, false, Runnable::run, 1000, MIN_BACKOFF, WAIT_WARNING);
      var a = watcher(sessionId(), pool, listener, false, Runnable::run, 1000, MIN_BACKOFF, WAIT_WARNING);
      var x1 = blobInfo("x1");
      x.enqueue(List.of(handle(x1)));
      waitFor(() -> eventsGrpcMock.openSubscriptions() == 1);

      // When: session A enqueues two outputs, one after the other, while it waits for the channel
      var a1 = blobInfo("a1");
      var a2 = blobInfo("a2");
      a.enqueue(List.of(handle(a1)));
      waitFor(() -> a.openSubscriptions() == 1);
      a.enqueue(List.of(handle(a2)));
      Thread.sleep(REOPEN_DELAY.toMillis() * 2);
      resultsGrpcMock.setStatus(x1.id(), RESULT_STATUS_COMPLETED);
      x.waitUntilIdle().toCompletableFuture().get(3, SECONDS);

      // Then: A opens one stream, for both outputs
      waitFor(() -> requestsWatching(a1).size() == 1);
      assertThat(requestsWatching(a1)).containsExactly(java.util.Set.of(a1.id().asString(), a2.id().asString()));
      assertThat(a.openSubscriptions()).isEqualTo(1);
    } finally {
      pool.close();
    }
  }

  @Test
  @DisplayName("should not reopen before the end of the backoff, even for new outputs")
  @Timeout(10)
  void should_not_reopen_before_the_end_of_the_backoff() throws Exception {
    // Given: every subscription fails, and the backoff after a failure lasts 1 s
    eventsGrpcMock.failingSubscriptions.set(1000);
    var watcher = watcher(sessionId, channelPool, listener, false, Runnable::run, 1000, Duration.ofSeconds(1), WAIT_WARNING);

    // When: a new output arrives every 20 ms for 600 ms
    for (int i = 0; i < 30; i++) {
      watcher.enqueue(List.of(handle(blobInfo("out-" + i))));
      Thread.sleep(20);
    }

    // Then: only the first subscription was attempted
    assertThat(eventsGrpcMock.requests).hasSize(1);
  }

  @Test
  @DisplayName("should report pending outputs at once when the pool closes during a backoff")
  @Timeout(10)
  void should_report_outputs_when_pool_closes_during_backoff() throws Exception {
    // Given: the subscription failed and waits out a 5 s backoff
    eventsGrpcMock.failingSubscriptions.set(1);
    var watcher = watcher(sessionId, channelPool, listener, false, Runnable::run, 1000, Duration.ofSeconds(5), WAIT_WARNING);
    var info = blobInfo("out");
    watcher.enqueue(List.of(handle(info)));
    waitFor(() -> eventsGrpcMock.requests.size() == 1 && watcher.openSubscriptions() == 0);
    var idle = watcher.waitUntilIdle().toCompletableFuture();

    // When
    var closing = System.nanoTime();
    channelPool.closeNow();

    // Then: the output is reported without waiting for the backoff
    idle.get(3, SECONDS);
    assertThat(System.nanoTime() - closing).isLessThan(SECONDS.toNanos(1));
    assertThat(listener.errors).hasSize(1);
    assertThat(listener.errors.get(0).cause()).hasMessageContaining("closed");
  }

  @Test
  @DisplayName("should warn, suggesting a larger event channel pool, when a subscription waits for a channel")
  @Timeout(10)
  void should_warn_when_waiting_for_a_channel() throws Exception {
    // Given: the only event channel is held by session X, and a warning delay of 100 ms
    var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(BlobCompletionEventWatcher.class);
    var level = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.WARN);
    logs.start();
    logger.addAppender(logs);
    var pool = newChannelPool(1);
    try {
      var x = watcher(sessionId(), pool, listener, false, Runnable::run, 1000, MIN_BACKOFF, WAIT_WARNING);
      x.enqueue(List.of(handle(blobInfo("x1"))));
      waitFor(() -> eventsGrpcMock.openSubscriptions() == 1);

      // When
      var a = watcher(sessionId(), pool, listener, false, Runnable::run, 1000, MIN_BACKOFF, Duration.ofMillis(100));
      a.enqueue(List.of(handle(blobInfo("a1"))));

      // Then
      waitFor(() -> logs.list.stream().anyMatch(event -> event.getLevel() == ch.qos.logback.classic.Level.WARN
        && event.getFormattedMessage().contains("withEventChannelPoolSize")));
    } finally {
      logger.detachAppender(logs);
      logger.setLevel(level);
      pool.close();
    }
  }

  private BlobCompletionEventWatcher watcher(BlobCompletionListener listener, boolean downloadData, Executor listenerExecutor, int maxIdsPerStream) {
    return watcher(sessionId, channelPool, listener, downloadData, listenerExecutor, maxIdsPerStream, MIN_BACKOFF, WAIT_WARNING);
  }

  private BlobCompletionEventWatcher watcher(SessionId session, ManagedChannelPool pool, BlobCompletionListener listener, boolean downloadData,
                                             Executor listenerExecutor, int maxIdsPerStream, Duration minBackoff, Duration waitWarning) {
    return new BlobCompletionEventWatcher(session, pool, listener, downloadData, listenerExecutor, scheduler,
      maxIdsPerStream, OPEN_DELAY, REOPEN_DELAY, minBackoff, minBackoff.compareTo(MAX_BACKOFF) > 0 ? minBackoff : MAX_BACKOFF, waitWarning);
  }

  /** The requests that watched {@code info}. */
  private List<java.util.Set<String>> requestsWatching(BlobInfo info) {
    return eventsGrpcMock.requests.stream().map(EventsGrpcMock::ids).filter(ids -> ids.contains(info.id().asString())).toList();
  }

  private BlobHandle handle(BlobInfo info) {
    return new BlobHandle(sessionId, completedFuture(info), channelPool);
  }

  /** Waits up to 3 seconds for a condition set by the watcher's threads. */
  private static void waitFor(BooleanSupplier condition) throws InterruptedException {
    var deadline = System.nanoTime() + SECONDS.toNanos(3);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() - deadline > 0) throw new AssertionError("condition not met within 3 s");
      Thread.sleep(5);
    }
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
