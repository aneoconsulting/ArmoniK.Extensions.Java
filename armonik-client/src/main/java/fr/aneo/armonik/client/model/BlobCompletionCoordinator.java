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

import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.internal.concurrent.Schedulers;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static java.lang.Math.min;
import static java.util.List.copyOf;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.*;

/**
 * Coordinator for managing batched blob completion monitoring operations within a session.
 * <p>
 * This class provides efficient batching and coordination of blob completion watching operations
 * for a session context. It manages multiple concurrent blob monitoring requests through a single
 * {@link BlobCompletionEventWatcher} instance, optimizing resource usage and network efficiency.
 * <p>
 * The coordinator operates on a batching model where:
 * <ul>
 *   <li>Blob handles are enqueued for monitoring</li>
 *   <li>Enqueued handles are batched according to configured policies</li>
 *   <li>Batches are submitted to the event watcher for cluster monitoring</li>
 *   <li>Completion events are delivered through the session's configured {@link BlobCompletionListener}</li>
 * </ul>
 * <p>
 * This class is used internally by {@link SessionHandle} to coordinate task output completion
 * monitoring and should not be used directly by client applications.
 *
 * @see BlobCompletionEventWatcher
 * @see BlobCompletionListener
 * @see SessionHandle#awaitOutputsProcessed()
 */
final class BlobCompletionCoordinator {

  private static final Logger logger = LoggerFactory.getLogger(BlobCompletionCoordinator.class);

  private final Semaphore permits;
  private final BlobCompletionEventWatcher watcher;
  private final Queue<CompletionStage<Void>> inFlightStages = new ConcurrentLinkedQueue<>();
  private final BatchingPolicy batchingPolicy;
  private final ScheduledExecutorService scheduler;
  private final Object lock = new Object();
  private final List<BlobHandle> buffer = new ArrayList<>();
  private final SessionId sessionId;
  private ScheduledFuture<?> timer;
  private CompletableFuture<Void> idleFuture;

  /**
   * Creates a new blob completion coordinator for the specified session.
   * <p>
   * This constructor initializes the coordinator with default batching policies
   * and a dedicated event watcher for efficient blob completion monitoring.
   * The coordinator will use the session's configured blob completion listener
   * for all completion event notifications.
   *
   * @param sessionId the identifier of the session this coordinator serves
   * @param channel   the gRPC channel for cluster communication
   * @throws NullPointerException if any parameter is null
   * @see SessionId
   * @see BlobCompletionListener
   * @see BlobCompletionEventWatcher
   */
  BlobCompletionCoordinator(SessionId sessionId, ManagedChannel channel, SessionDefinition sessionDefinition) {
    this(
      sessionId,
      new BlobCompletionEventWatcher(sessionId, channel, sessionDefinition.outputListener()),
      sessionDefinition.outputBatchingPolicy(),
      Schedulers.shared()
    );
  }

  /**
   * Creates a blob completion coordinator with custom batching configuration.
   * <p>
   * This constructor allows specification of custom batching policies and schedulers
   * for advanced coordination scenarios. It is primarily used for testing or when
   * specific batching behavior is required.
   *
   * @param sessionId      the identifier of the session this coordinator serves
   * @param watcher        the event watcher to use for blob completion monitoring
   * @param batchingPolicy the policy defining how blob handles are batched
   * @param scheduler      the executor service for batching timer operations
   * @throws NullPointerException if any parameter is null
   * @see BatchingPolicy
   * @see ScheduledExecutorService
   */
  BlobCompletionCoordinator(SessionId sessionId,
                            BlobCompletionEventWatcher watcher,
                            BatchingPolicy batchingPolicy,
                            ScheduledExecutorService scheduler
  ) {
    this.sessionId = requireNonNull(sessionId, "sessionId must not be null");
    this.watcher = requireNonNull(watcher, "watcher must not be null");
    this.batchingPolicy = requireNonNull(batchingPolicy, "batchingPolicy must not be null");
    this.scheduler = requireNonNull(scheduler, "scheduler must not be null");
    this.permits = new Semaphore(batchingPolicy.maxConcurrentBatches());
  }


  /**
   * Enqueues blob handles for completion monitoring.
   * <p>
   * This method adds the specified blob handles to the monitoring queue. The handles
   * will be batched according to the configured batching policy and submitted for
   * cluster monitoring. The session's blob completion listener will be notified
   * when these blobs reach terminal states.
   * <p>
   * The operation is asynchronous and non-blocking. Enqueued blob handles are
   * processed in the background according to the batching schedule.
   *
   * @param blobHandles the list of blob handles to monitor for completion
   * @throws NullPointerException if blobHandles is null
   * @see BlobHandle
   * @see BlobCompletionListener
   */
  public void enqueue(List<BlobHandle> blobHandles) {
    if (blobHandles != null && !blobHandles.isEmpty()) {
      boolean shouldArmTimer = false;
      boolean hitSizeTrigger;

      synchronized (lock) {
        if (buffer.isEmpty()) {
          shouldArmTimer = true;
        }
        buffer.addAll(blobHandles);
        final int bufferSize = buffer.size();

        if (bufferSize > 1000) {
          logger.warn("Blob buffer growing large (size={}). " +
              "This may indicate processing cannot keep up with submission rate.",
            bufferSize);
        }

        hitSizeTrigger = buffer.size() >= batchingPolicy.batchSize();

        if (shouldArmTimer && timer == null) {
          timer = scheduler.schedule(this::onTimer, batchingPolicy.maxDelay().toNanos(), NANOSECONDS);
        }
      }
      if (hitSizeTrigger) {
        flush();
      }
    }
  }

  /**
   * Returns a completion stage that completes when all currently tracked operations finish.
   * <p>
   * This method provides a synchronization point for waiting until all blob completion
   * operations that were enqueued at the time of invocation have reached terminal states.
   * The returned completion stage takes a snapshot of in-flight operations and excludes
   * any operations enqueued after this method is called.
   * <p>
   * If no operations are currently in progress, the returned completion stage is
   * already completed. This method does not cancel any ongoing operations.
   *
   * @return a completion stage that completes when all current operations finish,
   * or an already completed stage if no operations are in progress
   */
  public CompletionStage<Void> waitUntilIdle() {
    flush();
    synchronized (lock) {
      if (buffer.isEmpty() && inFlightStages.isEmpty()) {
        return completedFuture(null);
      }
      if (idleFuture == null || idleFuture.isDone()) {
        idleFuture = new CompletableFuture<>();
      }

      return idleFuture.handle((ok, ex) -> null);
    }
  }

  private void onTimer() {
    flush();
    maybeSignalIdle();
  }

  private void flush() {
    var drained = drainBatchLocked();
    if (!drained.isEmpty()) {
      var remaining = new ArrayList<List<BlobHandle>>();
      var batches = fixedSizeBatches(drained, batchingPolicy.capPerBatch());

      batches.forEach(batch -> {
          if (permits.tryAcquire()) {
            startWatch(batch);
          } else {
            remaining.add(batch);
          }
        }
      );

      if (!remaining.isEmpty()) {
        int totalBlobs = remaining.stream().mapToInt(List::size).sum();
        logger.warn("Batch concurrency limit reached (maxConcurrent={}). Buffering {} batches ({} blobs). " +
            "Consider increasing BatchingPolicy.maxConcurrentBatches if this happens frequently.",
          batchingPolicy.maxConcurrentBatches(),
          remaining.size(),
          totalBlobs);

        pushBackFront(remaining);
      }
    }
    maybeSignalIdle();
  }

  private List<BlobHandle> drainBatchLocked() {
    synchronized (lock) {
      List<BlobHandle> drained = List.of();
      if (!buffer.isEmpty()) {
        drained = copyOf(buffer);
        buffer.clear();
        if (timer != null) {
          timer.cancel(false);
          timer = null;
        }
      }
      return drained;
    }
  }

  private void startWatch(List<BlobHandle> batch) {
    var ticket = watcher.watch(batch);
    var stage = ticket.completion();
    inFlightStages.add(stage);

    ticket.leftoversAfterCompletion()
          .exceptionally(ex -> List.of())
          .whenComplete((leftovers, ex) -> {
            if (leftovers != null && !leftovers.isEmpty()) {
              logger.warn("Watch completed with {} leftover blobs (not observed). " +
                  "Blobs will be re-queued for retry. SessionId: {}",
                leftovers.size(),
                sessionId.asString());
              pushBackFront(List.of(leftovers));
            }
            permits.release();
            inFlightStages.remove(stage);
            flush();
            maybeSignalIdle();
          });
  }

  private void pushBackFront(List<List<BlobHandle>> chunks) {
    synchronized (lock) {
      var safeChunks = requireNonNullElse(chunks, List.<List<BlobHandle>>of());
      var head = safeChunks.stream()
                           .filter(c -> c != null && !c.isEmpty())
                           .flatMap(List::stream)
                           .collect(toCollection(ArrayList::new));

      if (!head.isEmpty()) {
        head.addAll(buffer);
        buffer.clear();
        buffer.addAll(head);
      }

      if (timer == null && !buffer.isEmpty()) {
        timer = scheduler.schedule(this::onTimer, batchingPolicy.maxDelay().toNanos(), NANOSECONDS);
      }
    }
  }

  private void maybeSignalIdle() {
    CompletableFuture<Void> toComplete = null;
    synchronized (lock) {
      if (idleFuture != null && !idleFuture.isDone()
        && buffer.isEmpty()
        && inFlightStages.isEmpty()) {
        toComplete = idleFuture;
      }
    }
    if (toComplete != null) {
      logger.info("All blob completion operations finished. SessionId: {}", sessionId.asString());
      toComplete.complete(null);
    }
  }


  private static List<List<BlobHandle>> fixedSizeBatches(List<BlobHandle> blobHandles, int batchSize) {
    if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be positive");

    var partitions = new ArrayList<List<BlobHandle>>();
    for (int i = 0; i < blobHandles.size(); i += batchSize) {
      partitions.add(copyOf(blobHandles.subList(i, min(i + batchSize, blobHandles.size()))));
    }

    return partitions;
  }
}
