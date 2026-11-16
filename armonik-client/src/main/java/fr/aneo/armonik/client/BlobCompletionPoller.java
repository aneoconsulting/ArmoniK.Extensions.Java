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
import fr.aneo.armonik.api.grpc.v1.FiltersCommon.FilterStatusOperator;
import fr.aneo.armonik.api.grpc.v1.FiltersCommon.FilterStringOperator;
import fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ListResultsRequest;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ListResultsResponse;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ResultRaw;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawEnumField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFilters;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.api.grpc.v1.sortdirection.SortDirectionOuterClass.SortDirection;
import fr.aneo.armonik.client.exception.ArmoniKException;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Waits for the outputs of a session by polling {@code ListResults}, as the C# and C++ SDKs do.
 * <p>
 * Watched outputs are kept in a set. While it is not empty, a poll lists those that are no longer
 * {@code CREATED}, in requests of at most {@link #MAX_IDS_PER_REQUEST} ids. Each completed output
 * is downloaded, its size known from the listing, and handed to the listener; each aborted output is
 * reported as failed. The delay between polls starts at {@link #MIN_INTERVAL} and doubles up to
 * {@link #MAX_INTERVAL} while no output progresses; any progress or new output resets it.
 * <p>
 * Every call is a short unary call or a download: no call stays open while outputs are awaited.
 */
final class BlobCompletionPoller {
  private static final Logger logger = LoggerFactory.getLogger(BlobCompletionPoller.class);

  static final Duration MIN_INTERVAL = Duration.ofMillis(100);
  static final Duration MAX_INTERVAL = Duration.ofSeconds(2);
  static final int MAX_IDS_PER_REQUEST = 1000;

  private final SessionId sessionId;
  private final ChannelPool channelPool;
  private final BlobCompletionListener listener;
  private final boolean downloadData;
  private final Executor listenerExecutor;
  private final ScheduledExecutorService scheduler;
  private final long minIntervalNanos;
  private final long maxIntervalNanos;

  /** Outputs not yet seen completed or aborted, by id. */
  private final Map<String, BlobHandle> pending = new ConcurrentHashMap<>();
  /** Work that may still add outputs or notify the listener: tracked stages, id resolutions, downloads and listener calls. */
  private final AtomicInteger inFlight = new AtomicInteger();

  private final Object lock = new Object();
  private boolean pollScheduled;
  private boolean stopped;
  private boolean resetInterval;
  private long intervalNanos;
  private CompletableFuture<Void> idleFuture;

  BlobCompletionPoller(SessionId sessionId,
                       ChannelPool channelPool,
                       BlobCompletionListener listener,
                       boolean downloadData,
                       Executor listenerExecutor,
                       ScheduledExecutorService scheduler) {
    this(sessionId, channelPool, listener, downloadData, listenerExecutor, scheduler, MIN_INTERVAL, MAX_INTERVAL);
  }

  BlobCompletionPoller(SessionId sessionId,
                       ChannelPool channelPool,
                       BlobCompletionListener listener,
                       boolean downloadData,
                       Executor listenerExecutor,
                       ScheduledExecutorService scheduler,
                       Duration minInterval,
                       Duration maxInterval) {
    this.sessionId = requireNonNull(sessionId, "sessionId must not be null");
    this.channelPool = requireNonNull(channelPool, "channelPool must not be null");
    this.listener = requireNonNull(listener, "listener must not be null");
    this.downloadData = downloadData;
    this.listenerExecutor = requireNonNull(listenerExecutor, "listenerExecutor must not be null");
    this.scheduler = requireNonNull(scheduler, "scheduler must not be null");
    this.minIntervalNanos = minInterval.toNanos();
    this.maxIntervalNanos = maxInterval.toNanos();
    this.intervalNanos = minIntervalNanos;
  }

  /**
   * Starts watching outputs, once their ids are known.
   *
   * @param blobHandles the outputs to watch
   */
  void enqueue(List<BlobHandle> blobHandles) {
    if (blobHandles == null) return;
    blobHandles.forEach(handle -> {
      inFlight.incrementAndGet();
      handle.deferredBlobInfo().whenComplete((info, error) -> {
        // An output without id has no task producing it: the failure is reported where the task fails.
        if (error == null) pending.put(info.id().asString(), handle);
        inFlight.decrementAndGet();
        schedulePoll(true);
        maybeSignalIdle();
      });
    });
  }

  /**
   * Counts a stage as in flight until it completes, so {@link #waitUntilIdle()} waits for it.
   *
   * @param stage a stage that may enqueue outputs or notify the listener
   */
  void track(CompletionStage<?> stage) {
    requireNonNull(stage, "stage must not be null");
    inFlight.incrementAndGet();
    stage.whenComplete((ignored, error) -> {
      inFlight.decrementAndGet();
      maybeSignalIdle();
    });
  }

  /**
   * Returns a stage completing once no output is pending and every tracked stage, download and
   * listener call has ended.
   *
   * @return a stage completing when the session's outputs are processed; it never fails
   */
  CompletionStage<Void> waitUntilIdle() {
    synchronized (lock) {
      if (isIdle()) return CompletableFuture.completedFuture(null);
      if (idleFuture == null || idleFuture.isDone()) idleFuture = new CompletableFuture<>();
      return idleFuture;
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

  private void schedulePoll(boolean newOutputs) {
    long delay;
    synchronized (lock) {
      if (newOutputs) resetInterval = true;
      if (stopped || pollScheduled || pending.isEmpty()) return;
      if (resetInterval) {
        intervalNanos = minIntervalNanos;
        resetInterval = false;
      }
      pollScheduled = true;
      delay = intervalNanos;
    }
    try {
      scheduler.schedule(this::poll, delay, NANOSECONDS);
    } catch (RuntimeException e) {
      synchronized (lock) {
        pollScheduled = false;
      }
      logger.error("Could not schedule the next poll of outputs. SessionId: {}", sessionId.asString(), e);
    }
  }

  private void poll() {
    var ids = new ArrayList<>(pending.keySet());
    var requests = new ArrayList<CompletableFuture<List<ResultRaw>>>();
    for (int start = 0; start < ids.size(); start += MAX_IDS_PER_REQUEST) {
      requests.add(list(ids.subList(start, Math.min(start + MAX_IDS_PER_REQUEST, ids.size()))).toCompletableFuture());
    }
    CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new))
                     .handle((ignored, error) -> {
                       var progress = false;
                       for (var request : requests) {
                         if (request.isCompletedExceptionally()) {
                           var failure = request.handle((value, ex) -> ex).join();
                           if (isPoolClosed(failure)) {
                             stop();
                             return progress;
                           }
                           logger.warn("Polling the outputs of session {} failed, retrying: {}", sessionId.asString(), failure.toString());
                           continue;
                         }
                         for (var result : request.join()) progress |= handle(result);
                       }
                       return progress;
                     })
                     .whenComplete((progress, error) -> {
                       synchronized (lock) {
                         pollScheduled = false;
                         if (!resetInterval) {
                           intervalNanos = Boolean.TRUE.equals(progress) ? minIntervalNanos : Math.min(intervalNanos * 2, maxIntervalNanos);
                         }
                       }
                       schedulePoll(false);
                       maybeSignalIdle();
                     });
  }

  private static boolean isPoolClosed(Throwable failure) {
    var cause = failure;
    while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) cause = cause.getCause();
    return cause instanceof IllegalStateException;
  }

  /**
   * Stops watching once the client is closed: no output can be listed or downloaded any more, so
   * {@link #waitUntilIdle()} is released instead of waiting for good.
   */
  private void stop() {
    synchronized (lock) {
      stopped = true;
    }
    if (!pending.isEmpty()) {
      logger.info("Client closed: stopped watching {} outputs of session {}", pending.size(), sessionId.asString());
      pending.clear();
    }
  }

  private CompletionStage<List<ResultRaw>> list(List<String> ids) {
    var request = ListResultsRequest.newBuilder()
                                    .setPage(0)
                                    .setPageSize(ids.size())
                                    .setFilters(finishedAmong(ids))
                                    // The control plane rejects a listing without sort
                                    .setSort(ListResultsRequest.Sort.newBuilder()
                                                                    .setField(ResultField.newBuilder().setResultRawField(ResultRawField.newBuilder().setField(ResultRawEnumField.RESULT_RAW_ENUM_FIELD_RESULT_ID)))
                                                                    .setDirection(SortDirection.SORT_DIRECTION_ASC))
                                    .build();
    try {
      return channelPool.executeAsync(channel -> Futures.toCompletionStage(ResultsGrpc.newFutureStub(channel).listResults(request)))
                        .thenApply(ListResultsResponse::getResultsList);
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Builds the filter selecting, among the given ids, the results that are no longer {@code CREATED}.
   */
  private static ResultsFilters.Filters finishedAmong(List<String> ids) {
    var notCreated = ResultsFilters.FilterField.newBuilder()
                                               .setField(ResultField.newBuilder().setResultRawField(ResultRawField.newBuilder().setField(ResultRawEnumField.RESULT_RAW_ENUM_FIELD_STATUS)))
                                               .setFilterStatus(ResultsFilters.FilterStatus.newBuilder()
                                                                                          .setOperator(FilterStatusOperator.FILTER_STATUS_OPERATOR_NOT_EQUAL)
                                                                                          .setValue(ResultStatus.RESULT_STATUS_CREATED))
                                               .build();
    var filters = ResultsFilters.Filters.newBuilder();
    for (var id : ids) {
      var isId = ResultsFilters.FilterField.newBuilder()
                                           .setField(ResultField.newBuilder().setResultRawField(ResultRawField.newBuilder().setField(ResultRawEnumField.RESULT_RAW_ENUM_FIELD_RESULT_ID)))
                                           .setFilterString(FilterString.newBuilder().setOperator(FilterStringOperator.FILTER_STRING_OPERATOR_EQUAL).setValue(id));
      filters.addOr(ResultsFilters.FiltersAnd.newBuilder().addAnd(isId).addAnd(notCreated));
    }
    return filters.build();
  }

  /**
   * Handles a listed result.
   *
   * @return true when it ended the wait for a pending output
   */
  private boolean handle(ResultRaw result) {
    switch (result.getStatus()) {
      case RESULT_STATUS_COMPLETED -> {
        var handle = pending.remove(result.getResultId());
        if (handle == null) return false;
        completed(handle, result.getSize());
        return true;
      }
      case RESULT_STATUS_ABORTED -> {
        var handle = pending.remove(result.getResultId());
        if (handle == null) return false;
        aborted(handle);
        return true;
      }
      default -> {
        return false;
      }
    }
  }

  private void completed(BlobHandle handle, long size) {
    if (!downloadData) return;
    inFlight.incrementAndGet();
    handle.deferredBlobInfo()
          .thenCompose(info -> handle.downloadData(size).handle((bytes, error) -> {
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

  private void aborted(BlobHandle handle) {
    inFlight.incrementAndGet();
    handle.deferredBlobInfo()
          .thenAccept(info -> notify(() -> listener.onError(new BlobCompletionListener.BlobError(info, new ArmoniKException("Blob aborted")))))
          .exceptionally(error -> {
            done();
            return null;
          });
  }

  /**
   * Runs a listener call on the listener executor, counting the output as processed once it returns.
   */
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
}
