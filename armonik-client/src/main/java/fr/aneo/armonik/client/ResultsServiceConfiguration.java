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

import fr.aneo.armonik.api.grpc.v1.Objects.Empty;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Reads the Results service configuration of the control plane once and shares it.
 * <p>
 * Its {@code DataChunkMaxSize} bounds the requests that carry blob data inline: a {@code CreateResults}
 * request no larger than this size replaces a metadata call followed by an upload stream per blob.
 * When the configuration cannot be read, the size is reported as 0, which disables inline creation:
 * for good when the control plane does not implement the call, for a minute after any other failure.
 */
final class ResultsServiceConfiguration {
  private static final Logger logger = LoggerFactory.getLogger(ResultsServiceConfiguration.class);
  static final Duration CALL_DEADLINE = Duration.ofSeconds(10);
  static final Duration RETRY_DELAY = Duration.ofMinutes(1);

  private final ChannelPool channelPool;
  private final LongSupplier nanoTime;
  private final AtomicReference<State> state = new AtomicReference<>();

  /**
   * A read in progress or done; a failed read is retried once {@code retryAfterNanos} has passed.
   */
  private record State(CompletableFuture<Integer> size, long retryAfterNanos) {
  }

  ResultsServiceConfiguration(ChannelPool channelPool) {
    this(channelPool, System::nanoTime);
  }

  ResultsServiceConfiguration(ChannelPool channelPool, LongSupplier nanoTime) {
    this.channelPool = requireNonNull(channelPool, "channelPool must not be null");
    this.nanoTime = requireNonNull(nanoTime, "nanoTime must not be null");
  }

  /**
   * Returns the maximum size, in bytes, of a request carrying blob data, as configured on the control plane.
   *
   * @return a stage completing with that size, or with 0 when the configuration could not be read; it never fails
   */
  CompletionStage<Integer> dataChunkMaxSize() {
    while (true) {
      var current = state.get();
      if (current != null && (current.retryAfterNanos() == Long.MAX_VALUE || nanoTime.getAsLong() - current.retryAfterNanos() < 0)) {
        return current.size();
      }
      var next = new State(new CompletableFuture<>(), Long.MAX_VALUE);
      if (state.compareAndSet(current, next)) {
        read(next);
        return next.size();
      }
    }
  }

  private void read(State reading) {
    try {
      channelPool.executeAsync(channel -> Futures.toCompletionStage(ResultsGrpc.newFutureStub(channel)
                                                                               .withDeadlineAfter(CALL_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                                                                               .getServiceConfiguration(Empty.getDefaultInstance())))
                 .whenComplete((response, error) -> {
                   if (error == null) {
                     logger.debug("Results service configuration read: DataChunkMaxSize={}", response.getDataChunkMaxSize());
                     reading.size().complete(response.getDataChunkMaxSize());
                   } else {
                     fallBack(reading, error);
                   }
                 });
    } catch (RuntimeException e) {
      fallBack(reading, e);
    }
  }

  private void fallBack(State reading, Throwable error) {
    if (Status.fromThrowable(error).getCode() == Status.Code.UNIMPLEMENTED) {
      logger.info("The control plane does not report its Results service configuration: blobs are created without inline data");
      reading.size().complete(0);
      return;
    }
    logger.warn("Could not read the Results service configuration, blobs are created without inline data for {}: {}", RETRY_DELAY, error.toString());
    state.compareAndSet(reading, new State(reading.size(), nanoTime.getAsLong() + RETRY_DELAY.toNanos()));
    reading.size().complete(0);
  }
}
