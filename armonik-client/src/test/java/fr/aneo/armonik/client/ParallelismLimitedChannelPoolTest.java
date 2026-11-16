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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static io.grpc.ConnectivityState.READY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ParallelismLimitedChannelPoolTest {

  private ParallelismLimitedChannelPool pool;

  @AfterEach
  void tearDown() {
    if (pool != null) pool.close();
  }

  @Test
  @DisplayName("should run at most the limit of calls at once, without blocking callers")
  @Timeout(5)
  void should_run_at_most_the_limit_of_calls_at_once() throws Exception {
    // Given
    pool = new ParallelismLimitedChannelPool(ManagedChannelPool.createUnbounded(this::channel), 2);
    var started = new AtomicInteger();
    var calls = range(0, 5).mapToObj(i -> new CompletableFuture<String>()).toList();

    // When: five calls are requested, each returning at once to the caller
    var results = calls.stream()
                       .map(call -> pool.executeAsync(channel -> {
                         started.incrementAndGet();
                         return call;
                       }).toCompletableFuture())
                       .toList();

    // Then: only two run
    assertThat(started.get()).isEqualTo(2);
    assertThat(results).noneMatch(CompletableFuture::isDone);

    // And when they end, the others run in turn
    calls.forEach(call -> call.complete("done"));
    awaitCondition(() -> started.get() == 5);
    for (var result : results) assertThat(result.get(2, SECONDS)).isEqualTo("done");
  }

  @Test
  @DisplayName("should fail calls still waiting for a permit on close")
  @Timeout(5)
  void should_fail_waiting_calls_on_close() {
    // Given
    pool = new ParallelismLimitedChannelPool(ManagedChannelPool.createUnbounded(this::channel), 1);
    pool.executeAsync(channel -> new CompletableFuture<>());
    var waiting = pool.executeAsync(channel -> CompletableFuture.completedFuture("never")).toCompletableFuture();

    // When
    pool.close();

    // Then
    assertThatThrownBy(() -> waiting.get(2, SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("should resolve 0 to the number of processors and a negative limit to none")
  void should_resolve_the_configured_limit() {
    assertThat(ParallelismLimitedChannelPool.resolve(0)).isEqualTo(Runtime.getRuntime().availableProcessors());
    assertThat(ParallelismLimitedChannelPool.resolve(-1)).isEqualTo(Integer.MAX_VALUE);
    assertThat(ParallelismLimitedChannelPool.resolve(8)).isEqualTo(8);
  }

  @Test
  @DisplayName("a call given up while waiting should neither run nor lose its permit")
  @Timeout(5)
  void call_given_up_while_waiting_should_not_lose_its_permit() throws Exception {
    // Given: a limit of 2, both permits in use, and two callers that give up after 200 ms
    pool = new ParallelismLimitedChannelPool(ManagedChannelPool.createUnbounded(this::channel), 2);
    var busy = range(0, 2).mapToObj(i -> new CompletableFuture<String>()).toList();
    busy.forEach(call -> pool.executeAsync(channel -> call));
    var ranGivenUp = new AtomicInteger();
    var givenUp = range(0, 2)
      .mapToObj(i -> pool.executeAsync(channel -> { ranGivenUp.incrementAndGet(); return CompletableFuture.completedFuture("x"); })
                         .toCompletableFuture()
                         .orTimeout(200, java.util.concurrent.TimeUnit.MILLISECONDS))
      .toList();
    givenUp.forEach(call -> assertThatThrownBy(() -> call.get(2, SECONDS)).hasCauseInstanceOf(java.util.concurrent.TimeoutException.class));

    // When: the busy calls end
    busy.forEach(call -> call.complete("done"));

    // Then: two calls can run at once again, and the given-up calls never ran
    var started = new AtomicInteger();
    var next = range(0, 2).mapToObj(i -> new CompletableFuture<String>()).toList();
    next.forEach(call -> pool.executeAsync(channel -> { started.incrementAndGet(); return call; }));
    awaitCondition(() -> started.get() == 2);
    assertThat(ranGivenUp.get()).isZero();
    next.forEach(call -> call.complete("done"));
  }

  @Test
  @DisplayName("an interrupted execute should not lose its permit")
  @Timeout(5)
  void interrupted_execute_should_not_lose_its_permit() throws Exception {
    // Given: a limit of 1 in use, and a caller waiting in execute()
    pool = new ParallelismLimitedChannelPool(ManagedChannelPool.createUnbounded(this::channel), 1);
    var busy = new CompletableFuture<String>();
    pool.executeAsync(channel -> busy);
    var waiting = new Thread(() -> {
      try {
        pool.execute(channel -> "never");
      } catch (RuntimeException expected) {
        // interrupted
      }
    });
    waiting.start();
    awaitCondition(() -> waiting.getState() == Thread.State.WAITING || waiting.getState() == Thread.State.TIMED_WAITING);

    // When
    waiting.interrupt();
    waiting.join(2000);
    busy.complete("done");

    // Then
    var next = pool.executeAsync(channel -> CompletableFuture.completedFuture("next")).toCompletableFuture();
    assertThat(next.get(2, SECONDS)).isEqualTo("next");
  }

  @Test
  @DisplayName("calls made after close should fail at once")
  @Timeout(5)
  void calls_after_close_should_fail_at_once() {
    // Given: a call still in flight when the pool closes
    pool = new ParallelismLimitedChannelPool(ManagedChannelPool.createUnbounded(this::channel), 1);
    pool.executeAsync(channel -> new CompletableFuture<>());
    pool.close();

    // When / Then
    var afterClose = pool.executeAsync(channel -> CompletableFuture.completedFuture("x")).toCompletableFuture();
    assertThat(afterClose).isCompletedExceptionally();
    assertThatThrownBy(() -> pool.execute(channel -> "x")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("execute should return the value of its operation")
  @Timeout(5)
  void execute_should_return_the_value_of_its_operation() {
    // Given
    pool = new ParallelismLimitedChannelPool(ManagedChannelPool.createUnbounded(this::channel), 1);

    // When
    var result = pool.execute(channel -> "done");

    // Then
    assertThat(result).isEqualTo("done");
  }

  @Test
  @DisplayName("an execute whose operation throws should return its permit")
  @Timeout(5)
  void execute_whose_operation_throws_should_return_its_permit() throws Exception {
    // Given
    pool = new ParallelismLimitedChannelPool(ManagedChannelPool.createUnbounded(this::channel), 1);

    // When
    assertThatThrownBy(() -> pool.execute(channel -> {
      throw new RuntimeException("operation failed");
    })).isInstanceOf(RuntimeException.class).hasMessage("operation failed");

    // Then: the only permit is back
    var next = pool.executeAsync(channel -> CompletableFuture.completedFuture("next")).toCompletableFuture();
    assertThat(next.get(2, SECONDS)).isEqualTo("next");
  }

  @Test
  @DisplayName("a call whose operation fails should return its permit")
  @Timeout(5)
  void call_whose_operation_fails_should_return_its_permit() throws Exception {
    // Given
    pool = new ParallelismLimitedChannelPool(ManagedChannelPool.createUnbounded(this::channel), 1);

    // When
    var failed = pool.executeAsync(channel -> CompletableFuture.<String>failedFuture(new RuntimeException("operation failed")))
                     .toCompletableFuture();
    assertThatThrownBy(() -> failed.get(2, SECONDS)).hasRootCauseMessage("operation failed");

    // Then: the only permit is back
    var next = pool.executeAsync(channel -> CompletableFuture.completedFuture("next")).toCompletableFuture();
    assertThat(next.get(2, SECONDS)).isEqualTo("next");
  }

  private ManagedChannel channel() {
    var channel = mock(ManagedChannel.class);
    when(channel.getState(false)).thenReturn(READY);
    when(channel.shutdown()).thenReturn(channel);
    return channel;
  }

  private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
    var deadline = System.nanoTime() + SECONDS.toNanos(2);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
    assertThat(condition.getAsBoolean()).isTrue();
  }
}
