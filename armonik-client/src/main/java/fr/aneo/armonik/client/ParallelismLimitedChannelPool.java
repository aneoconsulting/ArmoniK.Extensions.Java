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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Caps how many calls a client runs at once, whichever session or operation starts them.
 * <p>
 * Calls wait for a permit before taking a channel, without blocking any thread, except the
 * synchronous {@link #execute(Function)}, which blocks its caller.
 */
final class ParallelismLimitedChannelPool implements ChannelPool {
  private final ChannelPool delegate;
  private final AsyncLimiter permits;

  ParallelismLimitedChannelPool(ChannelPool delegate, int parallelismLimit) {
    if (parallelismLimit < 1) throw new IllegalArgumentException("parallelismLimit must be at least 1, got: " + parallelismLimit);
    this.delegate = requireNonNull(delegate, "delegate must not be null");
    this.permits = new AsyncLimiter(parallelismLimit);
  }

  /**
   * Resolves the limit set in {@link ArmoniKConfig}: 0 means the number of available processors,
   * a negative value means no limit.
   *
   * @param configured the configured parallelism limit
   * @return the limit to enforce, or {@link Integer#MAX_VALUE} for none
   */
  static int resolve(int configured) {
    if (configured < 0) return Integer.MAX_VALUE;
    if (configured == 0) return Runtime.getRuntime().availableProcessors();
    return configured;
  }

  /**
   * {@inheritDoc}
   * <p>
   * This synchronous variant blocks the calling thread until a permit is available.
   */
  @Override
  public <T> T execute(Function<ManagedChannel, T> operation) {
    requireNonNull(operation, "operation must not be null");
    var waiter = permits.acquire();
    try {
      waiter.get();
    } catch (InterruptedException e) {
      if (!permits.cancel(waiter)) permits.release();
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for a call permit", e);
    } catch (ExecutionException e) {
      throw Futures.asRuntimeException(e.getCause());
    }
    try {
      return delegate.execute(operation);
    } finally {
      permits.release();
    }
  }

  @Override
  public <T> CompletionStage<T> executeAsync(Function<ManagedChannel, CompletionStage<T>> operation) {
    requireNonNull(operation, "operation must not be null");
    return permits.withPermit(() -> delegate.executeAsync(operation));
  }

  @Override
  public void close() {
    permits.close(new IllegalStateException("Channel pool has been shut down"));
    delegate.close();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public int availableChannels() {
    return delegate.availableChannels();
  }

  @Override
  public RetryPolicy retryPolicy() {
    return delegate.retryPolicy();
  }
}
