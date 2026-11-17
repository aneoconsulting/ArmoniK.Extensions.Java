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

import fr.aneo.armonik.client.internal.concurrent.AsyncLimiter;
import fr.aneo.armonik.client.internal.concurrent.Futures;

import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
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
 *   <li><strong>Bounded:</strong> Maximum number of channels enforced. When the pool is exhausted,
 *   {@link #executeAsync(Function)} waits for a channel without blocking any thread, while the
 *   synchronous {@link #execute(Function)} blocks its caller.</li>
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
 * Each channel carries one call at a time. A call on a dedicated connection is not affected when
 * another connection is cut, for instance by an intermediate proxy, and shared HTTP/2 connections
 * caused problems in the C++ and C# SDKs. The cost is one TCP and TLS connection per call in flight,
 * and a throughput of at most about the pool size divided by the duration of a call.
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
  /**
   * How long a released channel may stay unused before the pool shuts it down. {@link ArmoniKClient}
   * also uses it as the idle timeout of its channels, after which gRPC closes their connection anyway.
   */
  static final Duration DEFAULT_MAX_IDLE = Duration.ofMinutes(5);

  private final Supplier<ManagedChannel> channelFactory;
  private final int maxSize;
  private final boolean isUnbounded;
  private final AsyncLimiter channelPermits;
  private final ConcurrentLinkedDeque<IdleChannel> availableChannels;
  private final long maxIdleNanos;
  private final LongSupplier nanoTime;
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
    this(maxSize, retryPolicy, channelFactory, DEFAULT_MAX_IDLE, System::nanoTime);
  }

  private ManagedChannelPool(int maxSize,
                             RetryPolicy retryPolicy,
                             Supplier<ManagedChannel> channelFactory,
                             Duration maxIdle,
                             LongSupplier nanoTime) {
    if (maxSize < 0) throw new IllegalArgumentException("maxSize must be non-negative, got: " + maxSize);

    this.channelFactory = requireNonNull(channelFactory, "channelFactory must not be null");
    this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy must not be null");
    this.maxSize = maxSize;
    this.isUnbounded = (maxSize == 0);
    this.channelPermits = new AsyncLimiter(isUnbounded ? Integer.MAX_VALUE : maxSize);
    this.availableChannels = new ConcurrentLinkedDeque<>();
    this.maxIdleNanos = requireNonNull(maxIdle, "maxIdle must not be null").toNanos();
    this.nanoTime = requireNonNull(nanoTime, "nanoTime must not be null");
    this.allChannels = ConcurrentHashMap.newKeySet();
    this.shutdown = new AtomicBoolean(false);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This synchronous variant blocks the calling thread until a channel is available.
   */
  @Override
  public <T> T execute(Function<ManagedChannel, T> operation) {
    requireNonNull(operation, "operation must not be null");
    ensureNotShutdown();

    var waiter = channelPermits.acquire();
    try {
      waiter.get();
    } catch (InterruptedException e) {
      if (!channelPermits.cancel(waiter)) channelPermits.release();
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while acquiring channel from pool", e);
    } catch (ExecutionException e) {
      throw Futures.asRuntimeException(e.getCause());
    }

    try {
      ensureNotShutdown();
      var channel = takeChannel();
      if (shutdown.get()) {
        // close() ran during takeChannel() and may have missed a channel it created: see runWithChannel.
        returnChannel(channel);
        throw new IllegalStateException("Channel pool has been shut down");
      }
      try {
        return operation.apply(channel);
      } finally {
        returnChannel(channel);
      }
    } finally {
      channelPermits.release();
    }
  }

  @Override
  public <T> CompletionStage<T> executeAsync(Function<ManagedChannel, CompletionStage<T>> operation) {
    requireNonNull(operation, "operation must not be null");
    ensureNotShutdown();

    return channelPermits.withPermit(() -> runWithChannel(operation));
  }

  /**
   * Runs an operation on a channel, for a caller that holds a channel permit; the caller returns the permit.
   */
  private <T> CompletableFuture<T> runWithChannel(Function<ManagedChannel, CompletionStage<T>> operation) {
    // A permit handed over by release() may arrive after close(): do not open a channel close() would miss.
    if (shutdown.get()) return CompletableFuture.failedFuture(new IllegalStateException("Channel pool has been shut down"));

    ManagedChannel channel;
    try {
      channel = takeChannel();
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
    // close() may also run during takeChannel(): a channel created there is already in allChannels, so either
    // close() saw it, or close() set shutdown before it was added and this check fails.
    if (shutdown.get()) {
      returnChannel(channel);
      return CompletableFuture.failedFuture(new IllegalStateException("Channel pool has been shut down"));
    }

    try {
      return operation.apply(channel)
                      .whenComplete((result, error) -> returnChannel(channel))
                      .toCompletableFuture();
    } catch (Exception e) {
      returnChannel(channel);
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public void close() {
    if (shutdown.compareAndSet(false, true)) {
      logger.info("Shutting down channel pool (size: {}, unbounded: {})", allChannels.size(), isUnbounded);

      var closed = new IllegalStateException("Channel pool has been shut down");
      channelPermits.close(closed);

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
   * Operations never wait for a channel to become available.
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
   * Each channel carries one call at a time.
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
   * Each channel carries one call at a time.
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
   * Creates a pool with an explicit idle eviction delay and time source, for tests.
   *
   * @param maxSize        maximum number of channels, 0 for unbounded
   * @param channelFactory factory for creating new gRPC channels
   * @param maxIdle        how long a released channel may stay unused before it is shut down
   * @param nanoTime       time source, in nanoseconds
   * @return a channel pool with the default retry policy
   */
  static ManagedChannelPool create(int maxSize, Supplier<ManagedChannel> channelFactory, Duration maxIdle, LongSupplier nanoTime) {
    return new ManagedChannelPool(maxSize, RetryPolicy.DEFAULT, channelFactory, maxIdle, nanoTime);
  }

  /**
   * Takes a healthy channel for a caller that already holds a channel permit.
   * <p>
   * Unhealthy channels found on the way are discarded; when no healthy channel is available, a new one is created.
   *
   * @return a healthy {@link ManagedChannel} ready for use
   */
  private ManagedChannel takeChannel() {
    evictIdleChannels();
    IdleChannel idle;
    while ((idle = availableChannels.pollFirst()) != null) {
      var candidate = idle.channel();
      if (isChannelHealthy(candidate)) {
        logger.trace("Reusing channel from pool. Pool size: {}", size());
        return candidate;
      }
      logger.debug("Discarding unhealthy channel from pool. Channel state: {}. Trying next...", candidate.getState(false));
      discard(candidate);
    }
    var channel = createChannel();
    logger.debug("Created new channel. Pool size: {}, max size: {}", size(), isUnbounded ? "unbounded" : maxSize);
    return channel;
  }

  /**
   * Returns a channel to the pool, or discards it when the pool is closed or the channel unhealthy.
   * The caller returns the permit it holds.
   *
   * @param channel the channel to return to the pool
   * @throws IllegalArgumentException if the channel was not acquired from this pool
   */
  private void returnChannel(ManagedChannel channel) {
    requireNonNull(channel, "channel must not be null");
    if (shutdown.get()) {
      // close() has already forgotten every channel: shut this one down without checking its origin.
      logger.trace("Pool is shutdown, closing released channel");
      allChannels.remove(channel);
      channel.shutdown();
      return;
    }
    if (!allChannels.contains(channel)) throw new IllegalArgumentException("Channel was not acquired from this pool");

    if (isChannelHealthy(channel)) {
      // Most recently used first: under a light load the same few channels serve every call,
      // and the others stay unused until evictIdleChannels shuts them down.
      availableChannels.offerFirst(new IdleChannel(channel, nanoTime.getAsLong()));
      logger.trace("Channel returned to the pool");
    } else {
      logger.debug("Discarding unhealthy channel on release. Channel state: {}", channel.getState(false));
      discard(channel);
    }
  }

  /**
   * Shuts down the available channels unused for longer than the maximum idle time, so the pool
   * shrinks back after a burst of calls. Released channels sit in the deque from most to least
   * recently used, so the scan starts from the tail and stops at the first channel still in use recently.
   */
  private void evictIdleChannels() {
    var now = nanoTime.getAsLong();
    var iterator = availableChannels.descendingIterator();
    while (iterator.hasNext()) {
      var idle = iterator.next();
      if (now - idle.releasedAt() < maxIdleNanos) break;
      // Each release adds a new entry, compared by identity: removing this one succeeds only if no
      // other thread has taken the channel since this release, and then no other thread can take it.
      if (availableChannels.removeLastOccurrence(idle)) {
        logger.debug("Shutting down channel unused for {}. Pool size: {}", Duration.ofNanos(now - idle.releasedAt()), size() - 1);
        discard(idle.channel());
      }
    }
  }

  /**
   * An available channel and the time it was released. Each release creates a new instance and
   * instances compare by identity, so an entry read by the evicter never matches a later release of
   * the same channel.
   */
  private static final class IdleChannel {
    private final ManagedChannel channel;
    private final long releasedAt;

    IdleChannel(ManagedChannel channel, long releasedAt) {
      this.channel = channel;
      this.releasedAt = releasedAt;
    }

    ManagedChannel channel() {
      return channel;
    }

    long releasedAt() {
      return releasedAt;
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

  /**
   * Drops a channel that carries no call, without waiting for its termination: this runs on
   * callers' and gRPC callback threads, which must never block.
   */
  private void discard(ManagedChannel channel) {
    allChannels.remove(channel);
    channel.shutdown();
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
