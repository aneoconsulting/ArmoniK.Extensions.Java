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

import fr.aneo.armonik.api.grpc.v1.FiltersCommon.FilterString;
import fr.aneo.armonik.api.grpc.v1.FiltersCommon.FilterStringOperator;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionRequest;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventsEnum;
import fr.aneo.armonik.api.grpc.v1.events.EventsGrpc;
import fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawEnumField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFilters;
import fr.aneo.armonik.client.exception.ArmoniKException;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Waits for the outputs of a session through {@code GetEvents} streams filtered on their ids.
 * <p>
 * A subscription watches up to {@link #MAX_IDS_PER_STREAM} outputs. When it opens, the control plane
 * registers its watchers, then sends the current status of every output of its filter, then every
 * status change: an output that completed before the subscription opened is therefore reported too.
 * New outputs are not watched by a stream of their own: they are added to the last subscription,
 * which is reopened with the outputs it still waits for plus the new ones. Reopenings are grouped:
 * new outputs wait {@link #REOPEN_DELAY} (or {@link #OPEN_DELAY} when no subscription is open) so
 * that one reopening covers every output enqueued meanwhile. Only the last subscription is reopened,
 * so a reopening costs the control plane a status query on at most {@link #MAX_IDS_PER_STREAM} ids,
 * whatever the number of pending outputs; a full subscription is never reopened to add outputs.
 * The new subscription is opened before the old one is cancelled; since the new one starts with the
 * current status of its outputs, nothing that changed in between is missed.
 * <p>
 * A subscription is cancelled as soon as none of its outputs is pending, since the control plane
 * never ends one on its own. A subscription that fails or ends is reopened with an exponential
 * backoff from {@link #MIN_BACKOFF} to {@link #MAX_BACKOFF}, reset by any event received; only a
 * status showing that reopening cannot succeed (invalid argument, unknown session, denied access,
 * unimplemented method) reports its outputs as failed. Once the client is closed, every output still
 * pending is reported as failed.
 * <p>
 * Each subscription holds one channel of the event channel pool for as long as it is open, so that
 * it never takes a channel from the calls of the client. Downloads use the client's channel pool.
 */
final class BlobCompletionEventWatcher {
  private static final Logger logger = LoggerFactory.getLogger(BlobCompletionEventWatcher.class);

  static final int MAX_IDS_PER_STREAM = 1000;
  static final Duration OPEN_DELAY = Duration.ofMillis(50);
  static final Duration REOPEN_DELAY = Duration.ofSeconds(1);
  static final Duration MIN_BACKOFF = Duration.ofSeconds(1);
  static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

  private final SessionId sessionId;
  private final ChannelPool eventChannelPool;
  private final BlobCompletionListener listener;
  private final boolean downloadData;
  private final Executor listenerExecutor;
  private final ScheduledExecutorService scheduler;
  private final int maxIdsPerStream;
  private final long openDelayNanos;
  private final long reopenDelayNanos;
  private final long minBackoffNanos;
  private final long maxBackoffNanos;

  private final Map<String, BlobHandle> pending = new ConcurrentHashMap<>();
  private final AtomicInteger inFlight = new AtomicInteger();

  private final Object lock = new Object();
  /** Open subscriptions, in the order they were opened; the last one is the only one reopened. */
  private final List<Subscription> subscriptions = new ArrayList<>();
  /** The subscription watching each pending output; outputs not yet watched have none. */
  private final Map<String, Subscription> owners = new HashMap<>();
  /** Pending outputs that no open subscription watches yet. */
  private final Set<String> unwatched = new LinkedHashSet<>();
  private ScheduledFuture<?> refresh;
  private long refreshAtNanos;
  private long backoffNanos;
  private boolean stopped;
  private CompletableFuture<Void> idleFuture;

  BlobCompletionEventWatcher(SessionId sessionId,
                             ChannelPool eventChannelPool,
                             BlobCompletionListener listener,
                             boolean downloadData,
                             Executor listenerExecutor,
                             ScheduledExecutorService scheduler) {
    this(sessionId, eventChannelPool, listener, downloadData, listenerExecutor, scheduler,
      MAX_IDS_PER_STREAM, OPEN_DELAY, REOPEN_DELAY, MIN_BACKOFF, MAX_BACKOFF);
  }

  BlobCompletionEventWatcher(SessionId sessionId,
                             ChannelPool eventChannelPool,
                             BlobCompletionListener listener,
                             boolean downloadData,
                             Executor listenerExecutor,
                             ScheduledExecutorService scheduler,
                             int maxIdsPerStream,
                             Duration openDelay,
                             Duration reopenDelay,
                             Duration minBackoff,
                             Duration maxBackoff) {
    this.sessionId = requireNonNull(sessionId, "sessionId must not be null");
    this.eventChannelPool = requireNonNull(eventChannelPool, "eventChannelPool must not be null");
    this.listener = requireNonNull(listener, "listener must not be null");
    this.downloadData = downloadData;
    this.listenerExecutor = requireNonNull(listenerExecutor, "listenerExecutor must not be null");
    this.scheduler = requireNonNull(scheduler, "scheduler must not be null");
    if (maxIdsPerStream < 1) throw new IllegalArgumentException("maxIdsPerStream must be at least 1, got: " + maxIdsPerStream);
    this.maxIdsPerStream = maxIdsPerStream;
    this.openDelayNanos = openDelay.toNanos();
    this.reopenDelayNanos = reopenDelay.toNanos();
    this.minBackoffNanos = minBackoff.toNanos();
    this.maxBackoffNanos = maxBackoff.toNanos();
    this.backoffNanos = minBackoffNanos;
  }

  /**
   * Starts watching outputs once their ids are known. An output whose id cannot be obtained has no
   * task producing it: its failure is reported where the task fails.
   */
  void enqueue(List<BlobHandle> blobHandles) {
    if (blobHandles == null) return;
    blobHandles.forEach(handle -> {
      inFlight.incrementAndGet();
      handle.deferredBlobInfo().whenComplete((info, error) -> {
        if (error != null) {
          done();
          return;
        }
        var id = info.id().asString();
        boolean watched;
        long delay = 0;
        synchronized (lock) {
          watched = !stopped;
          if (watched) {
            pending.put(id, handle);
            unwatched.add(id);
            delay = subscriptions.isEmpty() ? openDelayNanos : reopenDelayNanos;
          }
        }
        if (watched) {
          inFlight.decrementAndGet();
          scheduleRefresh(delay);
          maybeSignalIdle();
        } else {
          // The count taken above now covers the listener call
          failed(handle, clientClosed());
        }
      });
    });
  }

  /** Counts a stage as in flight until it completes, so that {@link #waitUntilIdle()} waits for it. */
  void track(CompletionStage<?> stage) {
    requireNonNull(stage, "stage must not be null");
    inFlight.incrementAndGet();
    stage.whenComplete((ignored, error) -> {
      inFlight.decrementAndGet();
      maybeSignalIdle();
    });
  }

  /** Completes once no output is pending and every listener call has returned. */
  CompletionStage<Void> waitUntilIdle() {
    synchronized (lock) {
      if (isIdle()) return CompletableFuture.completedFuture(null);
      if (idleFuture == null || idleFuture.isDone()) idleFuture = new CompletableFuture<>();
      return idleFuture;
    }
  }

  /** Number of open subscriptions, for tests. */
  int openSubscriptions() {
    synchronized (lock) {
      return subscriptions.size();
    }
  }

  private boolean isIdle() {
    return pending.isEmpty() && inFlight.get() == 0;
  }

  private void maybeSignalIdle() {
    CompletableFuture<Void> toComplete = null;
    synchronized (lock) {
      if (idleFuture != null && !idleFuture.isDone() && isIdle()) toComplete = idleFuture;
    }
    if (toComplete != null) toComplete.complete(null);
  }

  /** Schedules a refresh in {@code delayNanos}, or keeps the one already scheduled if it runs sooner. */
  private void scheduleRefresh(long delayNanos) {
    synchronized (lock) {
      if (stopped) return;
      var at = System.nanoTime() + delayNanos;
      if (refresh != null && !refresh.isDone()) {
        if (refreshAtNanos - at <= 0) return;
        refresh.cancel(false);
      }
      refreshAtNanos = at;
      try {
        refresh = scheduler.schedule(this::refresh, delayNanos, NANOSECONDS);
      } catch (RuntimeException e) {
        refresh = null;
        logger.error("Could not schedule the watch of outputs. SessionId: {}", sessionId.asString(), e);
      }
    }
  }

  /**
   * Watches the outputs no subscription watches yet: they join the last subscription if it is not
   * full, which is then reopened, and the rest go to new subscriptions.
   */
  private void refresh() {
    var toOpen = new ArrayList<Subscription>();
    var toCancel = new ArrayList<Subscription>();
    synchronized (lock) {
      refresh = null;
      if (stopped || unwatched.isEmpty()) return;
      var ids = new LinkedHashSet<String>();
      if (!subscriptions.isEmpty()) {
        var last = subscriptions.get(subscriptions.size() - 1);
        if (last.remaining < maxIdsPerStream) {
          subscriptions.remove(last);
          toCancel.add(last);
          last.ids.stream().filter(id -> owners.get(id) == last).forEach(ids::add);
        }
      }
      ids.addAll(unwatched);
      unwatched.clear();
      var chunk = new ArrayList<String>(Math.min(ids.size(), maxIdsPerStream));
      for (var id : ids) {
        chunk.add(id);
        if (chunk.size() == maxIdsPerStream) {
          toOpen.add(subscribe(chunk));
          chunk = new ArrayList<>(maxIdsPerStream);
        }
      }
      if (!chunk.isEmpty()) toOpen.add(subscribe(chunk));
    }
    toOpen.forEach(this::open);
    toCancel.forEach(Subscription::cancel);
  }

  /** Registers a subscription for {@code ids}; called with the lock held. */
  private Subscription subscribe(List<String> ids) {
    var subscription = new Subscription(List.copyOf(ids));
    subscriptions.add(subscription);
    ids.forEach(id -> owners.put(id, subscription));
    return subscription;
  }

  private void open(Subscription subscription) {
    var request = request(subscription.ids);
    CompletionStage<Void> stream;
    try {
      stream = eventChannelPool.executeAsync(channel -> {
        var ended = new CompletableFuture<Void>();
        if (subscription.isCancelled()) {
          ended.complete(null);
          return ended;
        }
        EventsGrpc.newStub(channel).getEvents(request, new Observer(subscription, ended));
        return ended;
      });
    } catch (RuntimeException e) {
      stream = CompletableFuture.failedFuture(e);
    }
    stream.whenComplete((ignored, error) -> {
      if (error != null) ended(subscription, unwrap(error));
    });
  }

  /** Handles the end of a subscription that was not cancelled by this watcher. */
  private void ended(Subscription subscription, Throwable cause) {
    if (subscription.isCancelled()) return;
    if (cause instanceof IllegalStateException || eventChannelPool.isClosed()) {
      stop();
      return;
    }
    var retry = cause == null || !isPermanent(cause);
    List<String> ids;
    long delay;
    synchronized (lock) {
      if (!subscriptions.remove(subscription)) return;
      ids = new ArrayList<>();
      for (var id : subscription.ids) {
        if (owners.get(id) == subscription) {
          owners.remove(id);
          ids.add(id);
        }
      }
      if (retry) unwatched.addAll(ids);
      delay = backoffNanos;
      if (retry) backoffNanos = Math.min(backoffNanos * 2, maxBackoffNanos);
    }
    if (retry) {
      logger.warn("Event stream of session {} ended, watching its {} outputs again in {} ms: {}",
        sessionId.asString(), ids.size(), NANOSECONDS.toMillis(delay), cause == null ? "completed by the server" : cause.toString());
      scheduleRefresh(delay);
      return;
    }
    logger.error("Event stream of session {} failed, reporting {} outputs as failed", sessionId.asString(), ids.size(), cause);
    for (var id : ids) {
      var handle = take(id);
      if (handle != null) failed(handle, new ArmoniKException("Could not watch the status of the output: " + cause, cause));
    }
  }

  private static Throwable unwrap(Throwable failure) {
    var cause = failure;
    while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
    return cause;
  }

  /**
   * A failed subscription says nothing about its outputs, which may still complete: it is reopened,
   * unless its status shows that reopening it cannot succeed.
   */
  private static boolean isPermanent(Throwable cause) {
    var code = Status.fromThrowable(cause).getCode();
    return code == Status.Code.INVALID_ARGUMENT
      || code == Status.Code.NOT_FOUND
      || code == Status.Code.PERMISSION_DENIED
      || code == Status.Code.UNAUTHENTICATED
      || code == Status.Code.UNIMPLEMENTED
      || code == Status.Code.FAILED_PRECONDITION;
  }

  private void stop() {
    List<Subscription> toCancel;
    synchronized (lock) {
      if (stopped) return;
      stopped = true;
      if (refresh != null) refresh.cancel(false);
      toCancel = new ArrayList<>(subscriptions);
      subscriptions.clear();
      owners.clear();
      unwatched.clear();
    }
    toCancel.forEach(Subscription::cancel);
    if (!pending.isEmpty()) {
      logger.info("Client closed: stopped watching {} outputs of session {}", pending.size(), sessionId.asString());
    }
    for (var id : new ArrayList<>(pending.keySet())) {
      var handle = take(id);
      if (handle != null) failed(handle, clientClosed());
    }
  }

  private static ArmoniKException clientClosed() {
    return new ArmoniKException("The client was closed before the output was available");
  }

  private EventSubscriptionRequest request(List<String> ids) {
    var filters = ResultsFilters.Filters.newBuilder();
    for (var id : ids) {
      var isId = ResultsFilters.FilterField.newBuilder()
                                           .setField(ResultField.newBuilder().setResultRawField(ResultRawField.newBuilder().setField(ResultRawEnumField.RESULT_RAW_ENUM_FIELD_RESULT_ID)))
                                           .setFilterString(FilterString.newBuilder().setOperator(FilterStringOperator.FILTER_STRING_OPERATOR_EQUAL).setValue(id));
      filters.addOr(ResultsFilters.FiltersAnd.newBuilder().addAnd(isId));
    }
    return EventSubscriptionRequest.newBuilder()
                                   .setSessionId(sessionId.asString())
                                   .setResultsFilters(filters)
                                   // NEW_RESULT makes the control plane send the current status of every output first
                                   .addReturnedEvents(EventsEnum.EVENTS_ENUM_NEW_RESULT)
                                   .addReturnedEvents(EventsEnum.EVENTS_ENUM_RESULT_STATUS_UPDATE)
                                   .build();
  }

  private void onStatus(String id, ResultStatus status) {
    switch (status) {
      case RESULT_STATUS_COMPLETED, RESULT_STATUS_ABORTED, RESULT_STATUS_DELETED, RESULT_STATUS_NOTFOUND -> {
        var handle = take(id);
        if (handle == null) return;
        if (status == ResultStatus.RESULT_STATUS_COMPLETED) {
          completed(handle);
        } else if (status == ResultStatus.RESULT_STATUS_ABORTED) {
          failed(handle, new ArmoniKException("Blob aborted"));
        } else {
          // Deleted (or gone) before it could be downloaded: it will never complete
          failed(handle, new ArmoniKException("Blob is no longer available, its status is " + status.name()));
        }
      }
      default -> {
      }
    }
  }

  /**
   * Removes an output from the pending set, counting it in flight first so that the watcher is never
   * seen idle before its listener call, and cancels its subscription once it waits for nothing more.
   */
  private BlobHandle take(String id) {
    inFlight.incrementAndGet();
    BlobHandle handle;
    Subscription finished = null;
    synchronized (lock) {
      handle = pending.remove(id);
      unwatched.remove(id);
      var owner = owners.remove(id);
      if (handle != null && owner != null && --owner.remaining == 0 && subscriptions.remove(owner)) finished = owner;
    }
    if (handle == null) done();
    if (finished != null) finished.cancel();
    return handle;
  }

  private void completed(BlobHandle handle) {
    if (!downloadData) {
      done();
      return;
    }
    handle.deferredBlobInfo()
          .thenCompose(info -> handle.downloadData().handle((bytes, error) -> {
            notify(error == null
              ? () -> listener.onSuccess(new BlobCompletionListener.Blob(info, bytes))
              : () -> listener.onError(new BlobCompletionListener.BlobError(info, error)));
            return null;
          }))
          .exceptionally(error -> {
            logger.error("Could not hand a completed output to the listener. SessionId: {}", sessionId.asString(), error);
            done();
            return null;
          });
  }

  private void failed(BlobHandle handle, Throwable cause) {
    handle.deferredBlobInfo()
          .thenAccept(info -> notify(() -> listener.onError(new BlobCompletionListener.BlobError(info, cause))))
          .exceptionally(error -> {
            done();
            return null;
          });
  }

  private void notify(Runnable call) {
    try {
      listenerExecutor.execute(() -> {
        try {
          call.run();
        } catch (Throwable throwable) {
          logger.error("Listener threw an exception. SessionId: {}", sessionId.asString(), throwable);
        } finally {
          done();
        }
      });
    } catch (RuntimeException rejected) {
      logger.error("The listener executor rejected a call. SessionId: {}", sessionId.asString(), rejected);
      done();
    }
  }

  private void done() {
    inFlight.decrementAndGet();
    maybeSignalIdle();
  }

  /** One {@code GetEvents} call and the outputs it watches. */
  private static final class Subscription {
    final List<String> ids;
    /** Outputs of this subscription still pending; guarded by the watcher's lock. */
    int remaining;
    private ClientCallStreamObserver<EventSubscriptionRequest> call;
    private boolean cancelled;

    Subscription(List<String> ids) {
      this.ids = ids;
      this.remaining = ids.size();
    }

    synchronized boolean isCancelled() {
      return cancelled;
    }

    synchronized void started(ClientCallStreamObserver<EventSubscriptionRequest> call) {
      this.call = call;
      if (cancelled) call.cancel("Outputs no longer watched by this stream", null);
    }

    void cancel() {
      ClientCallStreamObserver<EventSubscriptionRequest> toCancel;
      synchronized (this) {
        if (cancelled) return;
        cancelled = true;
        toCancel = call;
      }
      if (toCancel != null) toCancel.cancel("Outputs no longer watched by this stream", null);
    }
  }

  private final class Observer implements ClientResponseObserver<EventSubscriptionRequest, EventSubscriptionResponse> {
    private final Subscription subscription;
    private final CompletableFuture<Void> ended;

    Observer(Subscription subscription, CompletableFuture<Void> ended) {
      this.subscription = subscription;
      this.ended = ended;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<EventSubscriptionRequest> requestStream) {
      subscription.started(requestStream);
    }

    @Override
    public void onNext(EventSubscriptionResponse response) {
      synchronized (lock) {
        backoffNanos = minBackoffNanos;
      }
      if (response.hasNewResult()) {
        onStatus(response.getNewResult().getResultId(), response.getNewResult().getStatus());
      } else if (response.hasResultStatusUpdate()) {
        onStatus(response.getResultStatusUpdate().getResultId(), response.getResultStatusUpdate().getStatus());
      }
    }

    @Override
    public void onError(Throwable throwable) {
      // The channel goes back to the event pool, then the end is handled
      ended.complete(null);
      ended(subscription, throwable);
    }

    @Override
    public void onCompleted() {
      ended.complete(null);
      ended(subscription, null);
    }
  }
}
