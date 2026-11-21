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
import fr.aneo.armonik.api.grpc.v1.sortdirection.SortDirectionOuterClass.SortDirection;
import fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ListResultsRequest;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ListResultsResponse;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ResultRaw;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawEnumField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFilters;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.client.exception.ArmoniKException;
import fr.aneo.armonik.client.internal.concurrent.DownloadBudget;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import io.grpc.ManagedChannel;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Waits for the outputs of a session through {@code GetEvents} streams filtered on their ids.
 * <p>
 * A subscription watches up to {@link #MAX_IDS_PER_STREAM} outputs on one channel of the event
 * channel pool, which it keeps until none of its outputs is pending. When a call opens, the control
 * plane registers its watchers, then sends the current status of every output of its filter, then
 * every status change: an output that completed before the call opened is therefore reported too.
 * <p>
 * New outputs are not watched by a stream of their own: they join the last subscription, if it is not
 * full. A subscription still waiting for its channel simply takes them, since its request is built
 * when the channel is granted. A subscription already open is reopened on the channel it holds: its
 * call is cancelled and a new one is started with the outputs it still waits for plus the new ones,
 * and since the new call starts with the current status of its outputs, nothing that changed in
 * between is missed. Keeping the channel means that a session never loses it to the streams of other
 * sessions waiting for one. Reopenings are grouped: new outputs wait {@link #REOPEN_DELAY} (or
 * {@link #OPEN_DELAY} when no subscription is open) so that one reopening covers every output
 * enqueued meanwhile. Only the last subscription is reopened, so a reopening costs the control plane
 * a status query on at most {@link #MAX_IDS_PER_STREAM} ids, whatever the number of pending outputs.
 * <p>
 * A subscription is cancelled as soon as none of its outputs is pending, since the control plane
 * never ends one on its own. A subscription that fails or ends gives its channel back and is reopened
 * with an exponential backoff from {@link #MIN_BACKOFF} to {@link #MAX_BACKOFF}, reset by any event
 * received; no subscription is opened before the backoff ends, even for new outputs. Only a status
 * showing that reopening cannot succeed (invalid argument, unknown session, denied access,
 * unimplemented method) reports its outputs as failed. When the event channel pool closes, every
 * output still pending is reported as failed at once, whether or not its subscription is open.
 * <p>
 * Subscriptions take their channels from the event channel pool, so that they never take a channel
 * from the other calls of the client; downloads use the client's channel pool. When every event
 * channel is held, a subscription waits for one, possibly indefinitely if the streams holding them
 * wait for outputs that do not progress, such as those of paused sessions; a warning is logged once it
 * has waited {@link #WAIT_WARNING}.
 * <p>
 * With a limited {@link DownloadBudget}, a completed output is downloaded only once its size is
 * reserved in the budget, and the reservation is released once its listener call has returned, so
 * that the data the client holds stays bounded. The events do not carry the size of an output: the
 * sizes of completed outputs are read with one {@code ListResults} call per group, sent once
 * {@link #SIZE_BATCH} outputs are waiting or {@link #SIZE_WINDOW} after the first one. With the
 * unlimited budget, outputs are downloaded as soon as they complete, without reading their size.
 */
final class BlobCompletionEventWatcher {
  private static final Logger logger = LoggerFactory.getLogger(BlobCompletionEventWatcher.class);

  static final int MAX_IDS_PER_STREAM = 1000;
  static final Duration OPEN_DELAY = Duration.ofMillis(50);
  static final Duration REOPEN_DELAY = Duration.ofSeconds(1);
  static final Duration MIN_BACKOFF = Duration.ofSeconds(1);
  static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
  static final Duration WAIT_WARNING = Duration.ofSeconds(5);
  static final int SIZE_BATCH = 100;
  static final Duration SIZE_WINDOW = Duration.ofMillis(50);
  /** Deadline of a size lookup; a lookup that exceeds it is retried like any transient failure. */
  static final Duration SIZE_CALL_DEADLINE = Duration.ofSeconds(30);

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
  private final long waitWarningNanos;
  private final ChannelPool channelPool;
  private final DownloadBudget downloadBudget;
  /** The key of this session's reservations in the budget. */
  private final String budgetKey;
  private final int sizeBatch;
  private final long sizeWindowNanos;

  private final Map<String, BlobHandle> pending = new ConcurrentHashMap<>();
  private final AtomicInteger inFlight = new AtomicInteger();

  private final Object lock = new Object();
  /** Subscriptions waiting for a channel or open, in the order they were created; only the last one takes new outputs. */
  private final List<Subscription> subscriptions = new ArrayList<>();
  /** The subscription watching each pending output; outputs not yet watched have none. */
  private final Map<String, Subscription> owners = new HashMap<>();
  /** Pending outputs that no subscription watches yet. */
  private final Set<String> unwatched = new LinkedHashSet<>();
  private ScheduledFuture<?> refresh;
  private long refreshAtNanos;
  private long backoffNanos;
  /** Time before which no subscription is opened, after a failure; 0 when none. */
  private long retryAtNanos;
  /** Unregisters this watcher from the pool's close notification; registered only while outputs are pending. */
  private Runnable closeRegistration;
  private boolean registering;
  private boolean stopped;
  private CompletableFuture<Void> idleFuture;
  /** Completed outputs waiting for their size to be read. */
  private final List<Sizing> toSize = new ArrayList<>();
  private ScheduledFuture<?> sizeFlush;
  /** Groups whose size lookup failed and waits out a backoff before it is retried. */
  private final Map<List<Sizing>, ScheduledFuture<?>> sizeRetries = new HashMap<>();
  /** Completed outputs whose size is being read or whose download waits for the budget. */
  private int awaitingDownload;

  BlobCompletionEventWatcher(SessionId sessionId,
                             ChannelPool eventChannelPool,
                             BlobCompletionListener listener,
                             boolean downloadData,
                             Executor listenerExecutor,
                             ScheduledExecutorService scheduler) {
    this(sessionId, eventChannelPool, listener, downloadData, listenerExecutor, scheduler, eventChannelPool, DownloadBudget.unlimited());
  }

  BlobCompletionEventWatcher(SessionId sessionId,
                             ChannelPool eventChannelPool,
                             BlobCompletionListener listener,
                             boolean downloadData,
                             Executor listenerExecutor,
                             ScheduledExecutorService scheduler,
                             ChannelPool channelPool,
                             DownloadBudget downloadBudget) {
    this(sessionId, eventChannelPool, listener, downloadData, listenerExecutor, scheduler,
      MAX_IDS_PER_STREAM, OPEN_DELAY, REOPEN_DELAY, MIN_BACKOFF, MAX_BACKOFF, WAIT_WARNING,
      channelPool, downloadBudget, SIZE_BATCH, SIZE_WINDOW);
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
                             Duration maxBackoff,
                             Duration waitWarning) {
    this(sessionId, eventChannelPool, listener, downloadData, listenerExecutor, scheduler, maxIdsPerStream, openDelay,
      reopenDelay, minBackoff, maxBackoff, waitWarning, eventChannelPool, DownloadBudget.unlimited(), SIZE_BATCH, SIZE_WINDOW);
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
                             Duration maxBackoff,
                             Duration waitWarning,
                             ChannelPool channelPool,
                             DownloadBudget downloadBudget,
                             int sizeBatch,
                             Duration sizeWindow) {
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
    this.waitWarningNanos = waitWarning.toNanos();
    this.backoffNanos = minBackoffNanos;
    this.channelPool = requireNonNull(channelPool, "channelPool must not be null");
    this.downloadBudget = requireNonNull(downloadBudget, "downloadBudget must not be null");
    this.budgetKey = sessionId.asString();
    if (sizeBatch < 1) throw new IllegalArgumentException("sizeBatch must be at least 1, got: " + sizeBatch);
    this.sizeBatch = sizeBatch;
    this.sizeWindowNanos = sizeWindow.toNanos();
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
        boolean register = false;
        long delay = 0;
        synchronized (lock) {
          watched = !stopped;
          if (watched) {
            pending.put(id, handle);
            unwatched.add(id);
            delay = subscriptions.isEmpty() ? openDelayNanos : reopenDelayNanos;
            if (closeRegistration == null && !registering) {
              registering = true;
              register = true;
            }
          }
        }
        if (watched) {
          if (register) registerForClose();
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

  /** Number of subscriptions waiting for a channel or open, for tests. */
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

  /**
   * Registers {@link #stop()} to run when the event channel pool closes. Called without the lock,
   * since a pool already closed runs the listener at once.
   */
  private void registerForClose() {
    var registration = eventChannelPool.onClose(this::stop);
    Runnable toRemove = null;
    synchronized (lock) {
      registering = false;
      if ((pending.isEmpty() && awaitingDownload == 0) || stopped) toRemove = registration;
      else closeRegistration = registration;
    }
    if (toRemove != null) toRemove.run();
  }

  /**
   * Schedules a refresh in {@code delayNanos}, but not before the end of a running backoff, or keeps
   * the refresh already scheduled if it runs sooner.
   */
  private void scheduleRefresh(long delayNanos) {
    synchronized (lock) {
      if (stopped) return;
      var now = System.nanoTime();
      var at = now + delayNanos;
      if (retryAtNanos != 0 && retryAtNanos - at > 0) at = retryAtNanos;
      if (refresh != null && !refresh.isDone()) {
        if (refreshAtNanos - at <= 0) return;
        refresh.cancel(false);
      }
      refreshAtNanos = at;
      try {
        refresh = scheduler.schedule(this::refresh, Math.max(0, at - now), NANOSECONDS);
      } catch (RuntimeException e) {
        refresh = null;
        logger.error("Could not schedule the watch of outputs. SessionId: {}", sessionId.asString(), e);
      }
    }
  }

  /**
   * Watches the outputs no subscription watches yet: they join the last subscription if it is not
   * full, which is reopened on its channel if it is already open, and the rest go to new subscriptions.
   */
  private void refresh() {
    var toOpen = new ArrayList<Subscription>();
    Subscription toReopen = null;
    synchronized (lock) {
      refresh = null;
      if (stopped || unwatched.isEmpty()) return;
      if (retryAtNanos != 0) {
        if (retryAtNanos - System.nanoTime() > 0) {
          // Woken before the end of the backoff: wait for it
          scheduleRefresh(0);
          return;
        }
        retryAtNanos = 0;
      }
      var ids = new ArrayList<>(unwatched);
      unwatched.clear();
      var next = 0;
      if (!subscriptions.isEmpty()) {
        var last = subscriptions.get(subscriptions.size() - 1);
        while (next < ids.size() && last.ids.size() < maxIdsPerStream) {
          var id = ids.get(next++);
          last.ids.add(id);
          owners.put(id, last);
        }
        if (next > 0 && last.channel != null) toReopen = last;
      }
      while (next < ids.size()) {
        var subscription = new Subscription();
        while (next < ids.size() && subscription.ids.size() < maxIdsPerStream) {
          var id = ids.get(next++);
          subscription.ids.add(id);
          owners.put(id, subscription);
        }
        subscriptions.add(subscription);
        toOpen.add(subscription);
      }
    }
    if (toReopen != null) startCall(toReopen, toReopen.channel);
    toOpen.forEach(this::open);
  }

  /** Waits for a channel of the event pool, then starts the subscription's call on it and keeps it. */
  private void open(Subscription subscription) {
    scheduleWaitWarning(subscription);
    CompletionStage<Void> held;
    try {
      held = eventChannelPool.executeAsync(channel -> {
        startCall(subscription, channel);
        return subscription.released;
      });
    } catch (RuntimeException e) {
      held = CompletableFuture.failedFuture(e);
    }
    held.whenComplete((ignored, error) -> {
      if (error != null) ended(subscription, -1, unwrap(error));
    });
  }

  /**
   * Starts a call on {@code channel} for the outputs the subscription waits for, cancelling the call it
   * had, if any: the subscription keeps its channel across reopenings.
   */
  private void startCall(Subscription subscription, ManagedChannel channel) {
    ClientCallStreamObserver<EventSubscriptionRequest> previous;
    EventSubscriptionRequest request;
    Observer observer;
    synchronized (lock) {
      if (subscription.finished || subscription.ids.isEmpty()) {
        // Every output it waited for arrived before it got its channel
        subscription.finish();
        subscriptions.remove(subscription);
        return;
      }
      subscription.channel = channel;
      previous = subscription.call;
      subscription.call = null;
      subscription.generation++;
      request = request(List.copyOf(subscription.ids));
      observer = new Observer(subscription, subscription.generation);
    }
    if (previous != null) previous.cancel("Reopened with new outputs", null);
    EventsGrpc.newStub(channel).getEvents(request, observer);
  }

  /** Logs a warning once a subscription has waited {@link #WAIT_WARNING} for a channel. */
  private void scheduleWaitWarning(Subscription subscription) {
    try {
      scheduler.schedule(() -> {
        synchronized (lock) {
          if (stopped || subscription.finished || subscription.channel != null) return;
        }
        logger.warn("Outputs of session {} have waited {} s for an event channel: every channel of the event channel pool " +
            "({} open) is held by a stream waiting for outputs, for instance those of paused sessions, and new streams wait " +
            "until one closes. If this persists, raise the event channel pool size a little with ArmoniKConfig.withEventChannelPoolSize.",
          sessionId.asString(), NANOSECONDS.toSeconds(waitWarningNanos), eventChannelPool.size());
      }, waitWarningNanos, NANOSECONDS);
    } catch (RuntimeException ignored) {
      // The scheduler is shut down: the warning is not needed any more
    }
  }

  /**
   * Handles the end of a subscription's call of {@code generation} (-1 when it never got a channel)
   * that this watcher did not cancel: its channel goes back to the pool and its outputs are watched
   * again after a backoff, unless reopening cannot succeed.
   */
  private void ended(Subscription subscription, int generation, Throwable cause) {
    if (cause instanceof IllegalStateException || eventChannelPool.isClosed()) {
      stop();
      return;
    }
    var retry = cause == null || !isPermanent(cause);
    List<String> ids;
    long delay;
    synchronized (lock) {
      if (subscription.finished || (generation >= 0 && generation != subscription.generation)) return;
      subscription.finish();
      subscriptions.remove(subscription);
      ids = new ArrayList<>(subscription.ids);
      ids.forEach(owners::remove);
      subscription.ids.clear();
      delay = backoffNanos;
      if (retry) {
        unwatched.addAll(ids);
        backoffNanos = Math.min(backoffNanos * 2, maxBackoffNanos);
        retryAtNanos = System.nanoTime() + delay;
      }
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

  /** Stops watching, once the client is closed: every pending output is reported as failed. */
  private void stop() {
    List<Sizing> sizing;
    List<Subscription> toFinish;
    Runnable registration;
    synchronized (lock) {
      if (stopped) return;
      stopped = true;
      if (refresh != null) refresh.cancel(false);
      sizing = takeSizeBatch();
      sizeRetries.forEach((batch, retry) -> {
        retry.cancel(false);
        sizing.addAll(batch);
      });
      sizeRetries.clear();
      toFinish = new ArrayList<>(subscriptions);
      subscriptions.clear();
      owners.clear();
      unwatched.clear();
      registration = closeRegistration;
      closeRegistration = null;
      toFinish.forEach(Subscription::finish);
    }
    if (registration != null) registration.run();
    failSizing(sizing, clientClosed());
    downloadBudget.cancel(budgetKey, clientClosed());
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
        var handle = take(id, status == ResultStatus.RESULT_STATUS_COMPLETED && sizesNeeded());
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
   * seen idle before its listener call. A subscription that waits for nothing more is cancelled and
   * gives its channel back, and the watcher leaves the pool's close notification once nothing is pending.
   */
  private BlobHandle take(String id) {
    return take(id, false);
  }

  /**
   * Same as {@link #take(String)}; with {@code awaitDownload}, the output then counts as waiting for
   * its download, which keeps the watcher registered for the pool's close notification.
   */
  private BlobHandle take(String id, boolean awaitDownload) {
    inFlight.incrementAndGet();
    BlobHandle handle;
    Subscription finished = null;
    Runnable registration = null;
    synchronized (lock) {
      handle = pending.remove(id);
      if (handle != null && awaitDownload) awaitingDownload++;
      unwatched.remove(id);
      var owner = owners.remove(id);
      if (handle != null && owner != null) {
        owner.ids.remove(id);
        if (owner.ids.isEmpty() && subscriptions.remove(owner)) finished = owner;
      }
      if (pending.isEmpty() && awaitingDownload == 0) {
        registration = closeRegistration;
        closeRegistration = null;
      }
      if (finished != null) finished.finish();
    }
    if (handle == null) done();
    if (registration != null) registration.run();
    return handle;
  }

  private boolean sizesNeeded() {
    return downloadData && !downloadBudget.isUnlimited();
  }

  private void completed(BlobHandle handle) {
    if (!downloadData) {
      done();
      return;
    }
    if (sizesNeeded()) {
      handle.deferredBlobInfo()
            .thenAccept(info -> toSize(new Sizing(handle, info)))
            .exceptionally(error -> {
              logger.error("Could not hand a completed output to the listener. SessionId: {}", sessionId.asString(), error);
              downloadStarted();
              done();
              return null;
            });
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

  /** Buffers a completed output until its size is read, with the next group. */
  private void toSize(Sizing sizing) {
    List<Sizing> batch = null;
    boolean closed;
    synchronized (lock) {
      closed = stopped;
      if (!closed) {
        toSize.add(sizing);
        if (toSize.size() >= sizeBatch) {
          batch = takeSizeBatch();
        } else if (sizeFlush == null) {
          try {
            sizeFlush = scheduler.schedule(this::flushSizes, sizeWindowNanos, NANOSECONDS);
          } catch (RejectedExecutionException e) {
            batch = takeSizeBatch();
          }
        }
      }
    }
    if (closed) {
      downloadStarted();
      notifyError(sizing.info(), clientClosed());
    } else if (batch != null) {
      readSizes(batch, minBackoffNanos);
    }
  }

  /** Takes the outputs waiting for their size; called with the lock held. */
  private List<Sizing> takeSizeBatch() {
    var batch = new ArrayList<>(toSize);
    toSize.clear();
    if (sizeFlush != null) sizeFlush.cancel(false);
    sizeFlush = null;
    return batch;
  }

  private void flushSizes() {
    List<Sizing> batch;
    synchronized (lock) {
      batch = takeSizeBatch();
    }
    if (!batch.isEmpty()) readSizes(batch, minBackoffNanos);
  }

  /**
   * Reads the sizes of a group of completed outputs with one {@code ListResults} call, then starts
   * their downloads; a failed call is retried after {@code backoffNanos}, doubled at each failure.
   */
  private void readSizes(List<Sizing> batch, long backoffNanos) {
    synchronized (lock) {
      if (stopped) {
        failSizing(batch, clientClosed());
        return;
      }
    }
    var ids = batch.stream().map(sizing -> sizing.info().id().asString()).toList();
    var request = ListResultsRequest.newBuilder()
                                    .setPage(0)
                                    .setPageSize(ids.size())
                                    .setFilters(idsFilter(ids))
                                    // The control plane rejects a listing without sort
                                    .setSort(ListResultsRequest.Sort.newBuilder()
                                                                    .setField(ResultField.newBuilder().setResultRawField(ResultRawField.newBuilder().setField(ResultRawEnumField.RESULT_RAW_ENUM_FIELD_RESULT_ID)))
                                                                    .setDirection(SortDirection.SORT_DIRECTION_ASC))
                                    .build();
    CompletionStage<List<ResultRaw>> listing;
    try {
      listing = channelPool.executeAsync(channel -> Futures.toCompletionStage(ResultsGrpc.newFutureStub(channel)
                                                                                         .withDeadlineAfter(SIZE_CALL_DEADLINE.toMillis(), MILLISECONDS)
                                                                                         .listResults(request)))
                           .thenApply(ListResultsResponse::getResultsList);
    } catch (RuntimeException e) {
      listing = CompletableFuture.failedFuture(e);
    }
    listing.whenComplete((results, failure) -> {
      if (failure == null) {
        var sizes = new HashMap<String, Long>();
        results.forEach(result -> sizes.put(result.getResultId(), result.getSize()));
        for (var sizing : batch) {
          var size = sizes.get(sizing.info().id().asString());
          if (size != null) {
            download(sizing, size);
          } else {
            downloadStarted();
            notifyError(sizing.info(), new ArmoniKException("Blob is no longer available, its result was not found"));
          }
        }
        return;
      }
      var cause = unwrap(failure);
      boolean closed;
      synchronized (lock) {
        closed = stopped;
      }
      if (closed || cause instanceof IllegalStateException || channelPool.isClosed()) {
        failSizing(batch, clientClosed());
        return;
      }
      if (isPermanent(cause)) {
        logger.error("Could not read the size of {} outputs of session {}, reporting them as failed", batch.size(), sessionId.asString(), cause);
        failSizing(batch, new ArmoniKException("Could not read the size of the output: " + cause, cause));
        return;
      }
      logger.warn("Could not read the size of {} outputs of session {}, retrying in {} ms: {}",
        batch.size(), sessionId.asString(), NANOSECONDS.toMillis(backoffNanos), cause.toString());
      boolean rescheduled = false;
      synchronized (lock) {
        // Tracked, so that stop() reports the group at once rather than when the retry fires
        if (!stopped) {
          try {
            sizeRetries.put(batch, scheduler.schedule(() -> retrySizes(batch, Math.min(backoffNanos * 2, maxBackoffNanos)), backoffNanos, NANOSECONDS));
            rescheduled = true;
          } catch (RejectedExecutionException e) {
            // Reported below
          }
        }
      }
      if (!rescheduled) failSizing(batch, clientClosed());
    });
  }

  private void retrySizes(List<Sizing> batch, long backoffNanos) {
    synchronized (lock) {
      // Already reported by stop()
      if (sizeRetries.remove(batch) == null) return;
    }
    readSizes(batch, backoffNanos);
  }

  private void failSizing(List<Sizing> batch, Throwable cause) {
    for (var sizing : batch) {
      downloadStarted();
      notifyError(sizing.info(), cause);
    }
  }

  private static ResultsFilters.Filters idsFilter(List<String> ids) {
    var filters = ResultsFilters.Filters.newBuilder();
    for (var id : ids) {
      var isId = ResultsFilters.FilterField.newBuilder()
                                           .setField(ResultField.newBuilder().setResultRawField(ResultRawField.newBuilder().setField(ResultRawEnumField.RESULT_RAW_ENUM_FIELD_RESULT_ID)))
                                           .setFilterString(FilterString.newBuilder().setOperator(FilterStringOperator.FILTER_STRING_OPERATOR_EQUAL).setValue(id));
      filters.addOr(ResultsFilters.FiltersAnd.newBuilder().addAnd(isId));
    }
    return filters.build();
  }

  /**
   * Reserves the size of an output in the budget, then downloads it and hands it to the listener,
   * releasing the reservation once the listener call has returned.
   */
  private void download(Sizing sizing, long size) {
    CompletableFuture<Void> admission = null;
    synchronized (lock) {
      // Under the lock, so that stop() cancels every reservation made before it, and none is made after
      if (!stopped) admission = downloadBudget.acquire(budgetKey, size);
    }
    if (admission == null) {
      downloadStarted();
      notifyError(sizing.info(), clientClosed());
      return;
    }
    admission.whenComplete((admitted, refused) -> {
      downloadStarted();
      if (refused != null) {
        notifyError(sizing.info(), unwrap(refused));
        return;
      }
      Runnable release = () -> downloadBudget.release(budgetKey, size);
      CompletionStage<byte[]> data;
      try {
        data = sizing.handle().downloadData();
      } catch (RuntimeException e) {
        data = CompletableFuture.failedFuture(e);
      }
      data.whenComplete((bytes, error) -> notify(error == null
        ? () -> listener.onSuccess(new BlobCompletionListener.Blob(sizing.info(), bytes))
        : () -> listener.onError(new BlobCompletionListener.BlobError(sizing.info(), error)), release));
    });
  }

  /**
   * Counts a completed output as no longer waiting for its download, and leaves the pool's close
   * notification once nothing is pending or waiting.
   */
  private void downloadStarted() {
    Runnable registration = null;
    synchronized (lock) {
      awaitingDownload--;
      if (awaitingDownload == 0 && pending.isEmpty()) {
        registration = closeRegistration;
        closeRegistration = null;
      }
    }
    if (registration != null) registration.run();
  }

  private void notifyError(BlobInfo info, Throwable cause) {
    notify(() -> listener.onError(new BlobCompletionListener.BlobError(info, cause)));
  }

  private void notify(Runnable call) {
    notify(call, null);
  }

  /** Runs a listener call on the listener executor, then {@code after}, if any, then counts the output as processed. */
  private void notify(Runnable call, Runnable after) {
    Runnable counted = () -> {
      try {
        call.run();
      } catch (Throwable throwable) {
        logger.error("Listener threw an exception. SessionId: {}", sessionId.asString(), throwable);
      } finally {
        if (after != null) after.run();
        done();
      }
    };
    try {
      listenerExecutor.execute(counted);
    } catch (RejectedExecutionException rejected) {
      // A shut down or saturated executor must neither lose the output nor leave waiters hanging
      logger.warn("The listener executor rejected a call, running it on the current thread. SessionId: {}, cause: {}", sessionId.asString(), rejected.getMessage());
      counted.run();
    }
  }

  /** A completed output waiting for its size. */
  private record Sizing(BlobHandle handle, BlobInfo info) {
  }

  private void done() {
    inFlight.decrementAndGet();
    maybeSignalIdle();
  }

  /**
   * The outputs one {@code GetEvents} stream watches, and the channel it holds once granted. Every
   * field is guarded by the watcher's lock.
   */
  private static final class Subscription {
    /** Outputs of this subscription still pending. */
    final Set<String> ids = new LinkedHashSet<>();
    /** Completes once the subscription is finished, which gives its channel back to the pool. */
    final CompletableFuture<Void> released = new CompletableFuture<>();
    /** The channel granted by the pool; null while the subscription waits for one. */
    ManagedChannel channel;
    /** The current call, once started. */
    ClientCallStreamObserver<EventSubscriptionRequest> call;
    /** Incremented at each call started and when finished, so that the end of a replaced call is ignored. */
    int generation;
    boolean finished;

    /** Cancels the current call and gives the channel back; called with the watcher's lock held. */
    void finish() {
      if (finished) return;
      finished = true;
      generation++;
      var toCancel = call;
      call = null;
      if (toCancel != null) toCancel.cancel("Outputs no longer watched by this stream", null);
      released.complete(null);
    }
  }

  private final class Observer implements ClientResponseObserver<EventSubscriptionRequest, EventSubscriptionResponse> {
    private final Subscription subscription;
    private final int generation;

    Observer(Subscription subscription, int generation) {
      this.subscription = subscription;
      this.generation = generation;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<EventSubscriptionRequest> requestStream) {
      synchronized (lock) {
        if (!subscription.finished && subscription.generation == generation) {
          subscription.call = requestStream;
          return;
        }
      }
      // Replaced or finished before it started
      requestStream.cancel("Outputs no longer watched by this stream", null);
    }

    @Override
    public void onNext(EventSubscriptionResponse response) {
      synchronized (lock) {
        if (subscription.generation == generation) {
          backoffNanos = minBackoffNanos;
          retryAtNanos = 0;
        }
      }
      if (response.hasNewResult()) {
        onStatus(response.getNewResult().getResultId(), response.getNewResult().getStatus());
      } else if (response.hasResultStatusUpdate()) {
        onStatus(response.getResultStatusUpdate().getResultId(), response.getResultStatusUpdate().getStatus());
      }
    }

    @Override
    public void onError(Throwable throwable) {
      ended(subscription, generation, throwable);
    }

    @Override
    public void onCompleted() {
      ended(subscription, generation, null);
    }
  }
}
