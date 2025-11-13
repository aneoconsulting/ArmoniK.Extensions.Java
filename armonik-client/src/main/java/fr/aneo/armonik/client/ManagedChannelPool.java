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

import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Thread-safe implementation of {@link ChannelPool} for managing gRPC channel connections.
 * <p>
 * This pool manages gRPC channels, creating them on-demand up to a specified maximum (bounded mode)
 * or without limit (unbounded mode). Channels are validated before being returned to ensure they
 * are in a healthy state. Unhealthy channels are automatically discarded and replaced.
 * <p>
 * The pool can operate in two modes:
 * <ul>
 *   <li><strong>Bounded:</strong> Maximum number of channels enforced. Operations block when pool is exhausted.</li>
 *   <li><strong>Unbounded:</strong> No maximum limit. Channels created on-demand without blocking.</li>
 * </ul>
 * <p>
 * <strong>Key features:</strong>
 * <ul>
 *   <li>Thread-safe channel management using concurrent data structures</li>
 *   <li>Automatic channel health validation</li>
 *   <li>Lazy channel creation up to maximum pool size (or unlimited for unbounded)</li>
 *   <li>Graceful shutdown with timeout</li>
 *   <li>Functional API preventing resource leaks</li>
 *   <li>Configurable retry policy for handling transient failures</li>
 * </ul>
 * <p>
 * The pool also maintains a {@link RetryPolicy} that defines how operations should handle
 * and retry transient failures such as network issues or temporary unavailability.
 *
 * @see ChannelPool
 * @see ManagedChannel
 * @see RetryPolicy
 */
public final class ManagedChannelPool implements ChannelPool {
  private static final Logger logger = LoggerFactory.getLogger(ManagedChannelPool.class);
  private static final Duration GRACEFUL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(2);

  private final Supplier<ManagedChannel> channelFactory;
  private final int maxSize;
  private final boolean isUnbounded;
  private final Semaphore semaphore;
  private final ConcurrentLinkedQueue<ManagedChannel> availableChannels;
  private final Set<ManagedChannel> allChannels;
  private final AtomicBoolean shutdown;
  private final RetryPolicy retryPolicy;

  /**
   * Creates a new channel pool with the specified configuration.
   *
   * @param maxSize        maximum number of channels in the pool (0 for unbounded)
   * @param channelFactory factory for creating new gRPC channels
   * @throws IllegalArgumentException if maxSize is negative
   */
  private ManagedChannelPool(int maxSize, RetryPolicy retryPolicy, Supplier<ManagedChannel> channelFactory) {
    if (maxSize < 0) throw new IllegalArgumentException("maxSize must be non-negative, got: " + maxSize);

    this.channelFactory = requireNonNull(channelFactory, "channelFactory must not be null");
    this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy must not be null");
    this.maxSize = maxSize;
    this.isUnbounded = (maxSize == 0);
    this.semaphore = isUnbounded ? null : new Semaphore(maxSize);
    this.availableChannels = new ConcurrentLinkedQueue<>();
    this.allChannels = ConcurrentHashMap.newKeySet();
    this.shutdown = new AtomicBoolean(false);
  }

  @Override
  public <T> T execute(Function<ManagedChannel, T> operation) {
    requireNonNull(operation, "operation must not be null");

    ManagedChannel channel;
    try {
      channel = acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while acquiring channel from pool", e);
    }

    try {
      return operation.apply(channel);
    } finally {
      release(channel);
    }
  }

  @Override
  public <T> CompletionStage<T> executeAsync(Function<ManagedChannel, CompletionStage<T>> operation) {
    requireNonNull(operation, "operation must not be null");

    ManagedChannel channel;
    try {
      channel = acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return CompletableFuture.failedFuture(new RuntimeException("Interrupted while acquiring channel from pool", e));
    }

    try {
      return operation.apply(channel)
                      .whenComplete((result, error) -> release(channel));
    } catch (Exception e) {
      release(channel);
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public void close() {
    if (shutdown.compareAndSet(false, true)) {
      logger.info("Shutting down channel pool (size: {}, unbounded: {})", allChannels.size(), isUnbounded);

      var shutdownLatch = new CountDownLatch(allChannels.size());
      allChannels.forEach(channel ->
        CompletableFuture.runAsync(() -> {
          try {
            shutdownChannel(channel);
          } finally {
            shutdownLatch.countDown();
          }
        })
      );

      try {
        long timeoutMillis = GRACEFUL_SHUTDOWN_TIMEOUT.toMillis() * Math.max(1, allChannels.size());
        if (!shutdownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
          logger.warn("Timeout waiting for channels to shut down, forcing shutdown");
          allChannels.forEach(ManagedChannel::shutdownNow);
        }
      } catch (InterruptedException e) {
        logger.warn("Interrupted while shutting down channel pool, forcing immediate shutdown");
        Thread.currentThread().interrupt();
        allChannels.forEach(ManagedChannel::shutdownNow);
      }

      availableChannels.clear();
      allChannels.clear();
      logger.info("Channel pool shutdown complete");
    }
  }

  @Override
  public int size() {
    return allChannels.size();
  }

  @Override
  public int availableChannels() {
    return availableChannels.size();
  }

  @Override
  public RetryPolicy retryPolicy() {
    return retryPolicy;
  }

  /**
   * Returns whether this pool is unbounded (no maximum size limit).
   *
   * @return true if the pool can grow without limit, false otherwise
   */
  public boolean isUnbounded() {
    return isUnbounded;
  }

  /**
   * Creates an unbounded channel pool with a default retry policy.
   * <p>
   * An unbounded pool creates channels on-demand without any maximum limit.
   * Operations never block waiting for a channel to become available.
   * This is useful for highly concurrent scenarios where blocking is unacceptable.
   * <p>
   * The pool will use {@link RetryPolicy#DEFAULT} for handling transient failures.
   * <strong>Warning:</strong> Unbounded pools can create unlimited channels,
   * potentially exhausting system resources under high load. Use with caution
   * and ensure proper backpressure mechanisms are in place.
   *
   * @param channelFactory factory for creating new gRPC channels
   * @return an unbounded channel pool with a default retry policy
   * @throws NullPointerException if channelFactory is null
   */
  public static ManagedChannelPool createUnbounded(Supplier<ManagedChannel> channelFactory) {
    return new ManagedChannelPool(0, RetryPolicy.DEFAULT, channelFactory);
  }

  /**
   * Creates a bounded channel pool with custom size and default retry policy.
   * <p>
   * <strong>⚠️ Important: Pool Sizing for Event Streaming</strong>
   * <p>
   * When using event streams (e.g., {@link SessionHandle#awaitOutputsProcessed()}),
   * each stream holds a dedicated channel for the entire stream duration to prevent
   * channel state contamination. This means:
   * <ul>
   *   <li>Each active event stream occupies one channel from the pool</li>
   *   <li>Concurrent operations (downloads, submissions) need additional channels</li>
   *   <li>Pool must be sized to accommodate: <code>numStreams + numConcurrentOps</code></li>
   * </ul>
   * <p>
   * The pool will use {@link RetryPolicy#DEFAULT} for handling transient failures.
   * To specify a custom retry policy, use {@link #create(int, RetryPolicy, Supplier)}.
   *
   * @param maxSize        maximum number of channels in the pool (minimum 1)
   * @param channelFactory factory for creating new gRPC channels
   * @return a bounded channel pool with the default retry policy
   * @throws IllegalArgumentException if maxSize is less than 1
   * @throws NullPointerException if channelFactory is null
   */
  public static ManagedChannelPool create(int maxSize, Supplier<ManagedChannel> channelFactory) {
    return create(maxSize, RetryPolicy.DEFAULT, channelFactory);
  }

  /**
   * Creates a bounded channel pool with custom size and retry policy.
   * <p>
   * <strong>⚠️ Important: Pool Sizing for Event Streaming</strong>
   * <p>
   * When using event streams (e.g., {@link SessionHandle#awaitOutputsProcessed()}),
   * each stream holds a dedicated channel for the entire stream duration to prevent
   * channel state contamination. This means:
   * <ul>
   *   <li>Each active event stream occupies one channel from the pool</li>
   *   <li>Concurrent operations (downloads, submissions) need additional channels</li>
   *   <li>Pool must be sized to accommodate: <code>numStreams + numConcurrentOps</code></li>
   * </ul>
   * <p>
   * The specified retry policy will be used to handle transient failures during operations.
   * Pass {@code null} or {@link RetryPolicy#DEFAULT} to use default retry behavior.
   *
   * @param maxSize        maximum number of channels in the pool (minimum 1)
   * @param retryPolicy    retry policy for handling transient failures (null uses default)
   * @param channelFactory factory for creating new gRPC channels
   * @return a bounded channel pool with the specified retry policy
   * @throws IllegalArgumentException if maxSize is less than 1
   * @throws NullPointerException if channelFactory is null
   */
  public static ManagedChannelPool create(int maxSize, RetryPolicy retryPolicy, Supplier<ManagedChannel> channelFactory) {
    if (maxSize < 1) throw new IllegalArgumentException("maxSize must be at least 1 for bounded pool, got: " + maxSize);

    return new ManagedChannelPool(maxSize, retryPolicy, channelFactory);
  }

  /**
   * Acquires a {@link ManagedChannel} from the pool.
   * <p>
   * Behavior differs depending on the pool mode:
   * <ul>
   *   <li><strong>Bounded pool:</strong> If all channels are in use and the pool has reached its maximum size,
   *   this call blocks until a channel is released by another thread.</li>
   *   <li><strong>Unbounded pool:</strong> Always creates a new channel if none are available. Never blocks.</li>
   * </ul>
   * <p>
   * When acquiring a channel:
   * <ul>
   *   <li>The method polls the internal queue of available channels.</li>
   *   <li>Each polled channel is validated via {@link #isChannelHealthy(ManagedChannel)}.</li>
   *   <li>Unhealthy channels are automatically removed from the pool and shut down.</li>
   *   <li>The first healthy channel encountered is returned for reuse.</li>
   *   <li>If no healthy channel is found, a new one is created using the configured {@code channelFactory}.</li>
   * </ul>
   *
   * @return a healthy {@link ManagedChannel} ready for use
   * @throws IllegalStateException if the pool has been shut down
   * @throws InterruptedException  if interrupted while waiting for a channel in a bounded pool
   */
  private ManagedChannel acquire() throws InterruptedException {
    ensureNotShutdown();

    if (!isUnbounded) {
      semaphore.acquire();
    }
    try {
      ManagedChannel channel = null;
      ManagedChannel candidate;
      while (channel == null && (candidate = availableChannels.poll()) != null) {
        if (isChannelHealthy(candidate)) {
          channel = candidate;
          logger.trace("Reusing channel from pool. Pool size: {}", size());
        } else {
          logger.debug("Discarding unhealthy channel from pool. Channel state: {}. Trying next...",candidate.getState(false));
          shutdownChannel(candidate);
          allChannels.remove(candidate);
        }
      }
      if (channel == null) {
        channel = createChannel();
        logger.debug("Created new channel. Pool size: {}, max size: {}", size(), isUnbounded ? "unbounded" : maxSize);
      }
      return channel;
    } catch (Exception exception) {
      if (!isUnbounded) {
        semaphore.release();
      }
      throw exception;
    }
  }

  /**
   * Releases a channel back to the pool.
   *
   * @param channel the channel to return to the pool
   * @throws IllegalArgumentException if the channel was not acquired from this pool
   */
  private void release(ManagedChannel channel) {
    requireNonNull(channel, "channel must not be null");
    if (!allChannels.contains(channel)) throw new IllegalArgumentException("Channel was not acquired from this pool");

    try {
      if (shutdown.get()) {
        logger.trace("Pool is shutdown, closing released channel");
        shutdownChannel(channel);
        allChannels.remove(channel);
        return;
      }

      if (isChannelHealthy(channel)) {
        availableChannels.offer(channel);
        logger.trace("Channel returned to the pool");
      } else {
        logger.debug("Discarding unhealthy channel on release. Channel state: {}", channel.getState(false));
        shutdownChannel(channel);
        allChannels.remove(channel);
      }
    } finally {
      if (!isUnbounded) {
        semaphore.release();
      }
    }
  }

  private ManagedChannel createChannel() {
    var channel = channelFactory.get();
    allChannels.add(channel);
    return channel;
  }

  private boolean isChannelHealthy(ManagedChannel channel) {
    return switch (channel.getState(false)) {
      case READY, IDLE, CONNECTING -> true;
      case TRANSIENT_FAILURE, SHUTDOWN -> false;
    };
  }

  private void shutdownChannel(ManagedChannel channel) {
    try {
      channel.shutdown();
      long timeoutMillis = GRACEFUL_SHUTDOWN_TIMEOUT.toMillis();
      if (!channel.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
        logger.warn("Channel did not terminate within {}ms, forcing shutdown", timeoutMillis);
        channel.shutdownNow();
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted while shutting down channel");
      channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private void ensureNotShutdown() {
    if (shutdown.get()) {
      throw new IllegalStateException("Channel pool has been shut down");
    }
  }
}
