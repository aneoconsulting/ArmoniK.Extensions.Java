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

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ManagedChannelPoolTest {

  private ManagedChannelPool pool;

  @AfterEach
  void tearDown() {
    if (pool != null) {
      pool.close();
    }
  }

  @Test
  @DisplayName("execute should acquire channel apply operation and release channel")
  void execute_should_acquire_channel_apply_operation_and_release_channel() {
    // Given
    var channel = createHealthyChannel();
    pool = ManagedChannelPool.create(1, () -> channel);

    // When
    var result = pool.execute(ch -> "success");

    // Then
    assertThat(result).isEqualTo("success");
    assertThat(pool.availableChannels()).isEqualTo(1);
    verify(channel, atLeastOnce()).getState(false);
  }

  @Test
  @DisplayName("execute should release channel even when operation throws exception")
  void execute_should_release_channel_even_when_operation_throws_exception() {
    // Given
    var channel = createHealthyChannel();
    pool = ManagedChannelPool.create(1, () -> channel);

    // When/Then
    assertThatThrownBy(() -> pool.execute(ch -> {
        throw new RuntimeException("operation failed");
      })
    ).isInstanceOf(RuntimeException.class);
    assertThat(pool.availableChannels()).isEqualTo(1);
  }

  @Test
  @DisplayName("execute async should acquire channel and release on completion")
  void execute_async_should_acquire_channel_and_release_on_completion() throws Exception {
    // Given
    var channel = createHealthyChannel();
    pool = ManagedChannelPool.create(1, () -> channel);

    // When
    var completionStage = pool.executeAsync(ch -> CompletableFuture.supplyAsync(() -> "async success"));

    // Then
    assertThat(completionStage.toCompletableFuture().get(1, SECONDS)).isEqualTo("async success");
    assertThat(pool.availableChannels()).isEqualTo(1);
  }

  @Test
  @DisplayName("execute async should release channel even when operation fails")
  void execute_async_should_release_channel_even_when_operation_fails() throws Exception {
    // Given
    var channel = createHealthyChannel();
    pool = ManagedChannelPool.create(1, () -> channel);

    // When
    var completionStage = pool.executeAsync(ch -> CompletableFuture.supplyAsync(() -> {
        throw new RuntimeException("async operation failed");
      })
    );

    // Then
    assertThatThrownBy(() -> completionStage.toCompletableFuture().get(1, SECONDS))
      .hasCauseInstanceOf(RuntimeException.class);

    Thread.sleep(100);
    assertThat(pool.availableChannels()).isEqualTo(1);
  }

  @Test
  @DisplayName("pool should create channels lazily up to max size")
  void pool_should_create_channels_lazily_up_to_max_size() {
    // Given
    var creationCount = new AtomicInteger(0);
    pool = ManagedChannelPool.create(3, () -> {
      creationCount.incrementAndGet();
      return createHealthyChannel();
    });

    // When
    assertThat(pool.size()).isEqualTo(0);
    assertThat(creationCount.get()).isEqualTo(0);

    // First execution creates one channel
    pool.execute(ch -> "first");
    assertThat(pool.size()).isEqualTo(1);
    assertThat(creationCount.get()).isEqualTo(1);

    // Concurrent executions create more channels up to max
    var executor = Executors.newFixedThreadPool(3);
    var latch = new CountDownLatch(3);
    var releaseLatch = new CountDownLatch(1);

    range(0, 3).<Callable<String>>mapToObj(i -> () -> pool.execute(ch -> {
      latch.countDown();
      try {
        releaseLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "concurrent";
    })).forEach(executor::submit);

    // Then
    try {
      assertThat(latch.await(2, SECONDS)).isTrue();
      assertThat(pool.size()).isEqualTo(3);
      assertThat(creationCount.get()).isEqualTo(3);
    } catch (InterruptedException e) {
      fail("Test interrupted");
    } finally {
      releaseLatch.countDown();
      executor.shutdown();
    }
  }

  @Test
  @DisplayName("pool should reuse available channels before creating new ones")
  void pool_should_reuse_available_channels_before_creating_new_ones() {
    // Given
    var creationCount = new AtomicInteger(0);
    pool = ManagedChannelPool.create(5, () -> {
      creationCount.incrementAndGet();
      return createHealthyChannel();
    });

    // When - execute operations sequentially
    range(0, 10).forEach(i -> pool.execute(ch -> "operation-" + i));

    // Then - only one channel should be created and reused
    assertThat(pool.size()).isEqualTo(1);
    assertThat(creationCount.get()).isEqualTo(1);
  }

  @Test
  @DisplayName("pool should discard unhealthy channels and create new ones")
  void pool_should_discard_unhealthy_channels_and_create_new_ones() {
    // Given
    var unhealthyChannel = createUnhealthyChannel();
    var healthyChannel = createHealthyChannel();
    var calls = new AtomicInteger(0);

    pool = ManagedChannelPool.create(1, () -> calls.incrementAndGet() == 1 ? unhealthyChannel : healthyChannel);

    // When - first execution gets unhealthy channel
    pool.execute(ch -> "first");

    // Then - unhealthy channel should be discarded
    verify(unhealthyChannel).shutdown();

    // When - second execution should get new healthy channel
    pool.execute(ch -> "second");

    // Then
    assertThat(pool.size()).isEqualTo(1);
    assertThat(calls.get()).isEqualTo(2);
  }

  @Test
  @DisplayName("concurrent threads should wait when pool is exhausted")
  @Timeout(5)
  void concurrent_threads_should_wait_when_pool_is_exhausted() throws Exception {
    // Given
    pool = ManagedChannelPool.create(2, this::createHealthyChannel);
    var executor = Executors.newFixedThreadPool(5);
    var allThreadsStarted = new CountDownLatch(5);
    var firstTwoAcquired = new CountDownLatch(2);
    var releaseChannels = new CountDownLatch(1);
    var futures = IntStream.range(0, 5)
                           .mapToObj(taskId -> executor.submit(() -> {
                             allThreadsStarted.countDown();
                             return pool.execute(ch -> {
                               boolean shouldWait = firstTwoAcquired.getCount() > 0;
                               firstTwoAcquired.countDown();
                               if (shouldWait) {
                                 try {
                                   releaseChannels.await();
                                 } catch (InterruptedException e) {
                                   Thread.currentThread().interrupt();
                                 }
                               }
                               return "task-" + taskId;
                             });
                           })).toList();

    // When - all threads start
    assertThat(allThreadsStarted.await(1, SECONDS)).isTrue();

    // Then - only 2 channels are in use (pool max)
    assertThat(firstTwoAcquired.await(1, SECONDS)).isTrue();
    assertThat(pool.size()).isEqualTo(2);
    assertThat(pool.availableChannels()).isEqualTo(0);

    // And when - release the first two threads
    releaseChannels.countDown();

    // Then - all 5 tasks eventually complete
    futures.forEach(future -> {
      try {
        assertThat(future.get(3, SECONDS)).startsWith("task-");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    executor.shutdown();
  }

  @Test
  @DisplayName("should throw exception when pool is shutdown")
  void should_throw_exception_when_pool_is_shutdown() {
    // Given
    pool = ManagedChannelPool.create(1, this::createHealthyChannel);

    // When
    pool.close();

    // Then
    assertThatThrownBy(() -> pool.execute(ch -> "test"))
      .isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("onClose should run its listener once, on close or closeNow")
  void on_close_should_run_listener_on_close_and_close_now() {
    // Given
    var onClose = new AtomicInteger();
    var onCloseNow = new AtomicInteger();
    var closed = ManagedChannelPool.create(1, this::createHealthyChannel);
    pool = ManagedChannelPool.create(1, this::createHealthyChannel);
    closed.onClose(onClose::incrementAndGet);
    pool.onClose(onCloseNow::incrementAndGet);

    // When
    closed.close();
    closed.close();
    pool.closeNow();

    // Then
    assertThat(onClose).hasValue(1);
    assertThat(onCloseNow).hasValue(1);
  }

  @Test
  @DisplayName("onClose should run its listener at once when the pool is already closed")
  void on_close_should_run_listener_at_once_when_already_closed() {
    // Given
    var calls = new AtomicInteger();
    pool = ManagedChannelPool.create(1, this::createHealthyChannel);
    pool.close();

    // When
    pool.onClose(calls::incrementAndGet);

    // Then
    assertThat(calls).hasValue(1);
  }

  @Test
  @DisplayName("onClose should not run a listener that was unregistered")
  void on_close_should_not_run_unregistered_listener() {
    // Given
    var calls = new AtomicInteger();
    pool = ManagedChannelPool.create(1, this::createHealthyChannel);
    var unregister = pool.onClose(calls::incrementAndGet);

    // When
    unregister.run();
    pool.close();

    // Then
    assertThat(calls).hasValue(0);
  }

  @Test
  @DisplayName("close should shutdown all channels gracefully")
  void close_should_shutdown_all_channels_gracefully() {
    // Given
    var channel = createHealthyChannel();
    pool = ManagedChannelPool.create(1, () -> channel);
    pool.execute(ch -> "test");

    // When
    pool.close();

    // Then
    verify(channel).shutdown();
    assertThat(pool.size()).isEqualTo(0);
    assertThat(pool.availableChannels()).isEqualTo(0);
  }

  @Test
  @DisplayName("execute async should handle immediate exception in operation")
  void execute_async_should_handle_immediate_exception_in_operation() throws Exception {
    // Given
    var channel = createHealthyChannel();
    pool = ManagedChannelPool.create(1, () -> channel);

    // When
    var completionStage = pool.executeAsync(ch -> {
      throw new IllegalStateException("immediate failure");
    });

    // Then
    assertThatThrownBy(() -> completionStage.toCompletableFuture().get(1, SECONDS))
      .hasCauseInstanceOf(IllegalStateException.class);
    Thread.sleep(100);
    assertThat(pool.availableChannels()).isEqualTo(1);
  }

  @Test
  @DisplayName("pool should handle channel shutdown failure gracefully")
  void pool_should_handle_channel_shutdown_failure_gracefully() throws Exception {
    // Given
    var channel = createHealthyChannel();
    when(channel.awaitTermination(anyLong(), any())).thenReturn(false);
    pool = ManagedChannelPool.create(1, () -> channel);
    pool.execute(ch -> "test");

    // When
    pool.close();

    // Then
    verify(channel).shutdown();
    verify(channel).shutdownNow();
  }

  @Test
  @DisplayName("pool should evict two returned unhealthy channels then reuse the later-returned healthy one (no new creation)")
  void pool_should_reuse_healthy_released_after_two_unhealthy() {
    // Given
    var degradingChannel1 = createHealthyChannel();
    var degradingChannel2 = createHealthyChannel();
    var healthyChannel = createHealthyChannel();
    var calls = new AtomicInteger(0);

    pool = ManagedChannelPool.create(5, () -> {
      var call = calls.incrementAndGet();
      return switch (call) {
        case 1 -> degradingChannel1;
        case 2 -> degradingChannel2;
        case 3 -> healthyChannel;
        default -> throw new IllegalStateException("Unexpected call: " + call);
      };
    });
    var acquired = new CountDownLatch(3);
    var operation1 = new CompletableFuture<>();
    var operation2 = new CompletableFuture<>();
    var operation3 = new CompletableFuture<>();

    pool.executeAsync(executeOperation(acquired, operation1));
    pool.executeAsync(executeOperation(acquired, operation2));
    pool.executeAsync(executeOperation(acquired, operation3));
    // Released channels are reused most recent first: release the healthy one first, so the two
    // channels that degrade are found, and discarded, before it.
    operation3.complete("ok-3");
    operation2.complete("ok-2");
    operation1.complete("ok-1");

    // Degrade channel 1 and channel 2
    when(degradingChannel1.getState(false)).thenReturn(TRANSIENT_FAILURE);
    when(degradingChannel2.getState(false)).thenReturn(TRANSIENT_FAILURE);

    // Sanity: all completed and available
    assertThat(pool.availableChannels()).isEqualTo(3);

    // When
    var usedChannel = pool.execute(ch -> ch);

    // Then
    assertThat(usedChannel).isSameAs(healthyChannel);
    verify(degradingChannel1).shutdown();
    verify(degradingChannel2).shutdown();
    verify(healthyChannel, never()).shutdown();
    assertThat(calls.get()).isEqualTo(3);
    assertThat(pool.size()).isEqualTo(1);
  }

  @Test
  @DisplayName("execute async should not block the caller when the pool is exhausted")
  @Timeout(5)
  void execute_async_should_not_block_the_caller_when_pool_is_exhausted() throws Exception {
    // Given
    pool = ManagedChannelPool.create(1, this::createHealthyChannel);
    var holding = new CompletableFuture<String>();
    pool.executeAsync(ch -> holding);

    // When: the only channel is busy
    var waiting = pool.executeAsync(ch -> CompletableFuture.completedFuture("second")).toCompletableFuture();

    // Then: the call returned at once, and its operation runs once the channel is released
    assertThat(waiting).isNotDone();
    holding.complete("first");
    assertThat(waiting.get(2, SECONDS)).isEqualTo("second");
    assertThat(pool.size()).isEqualTo(1);
  }

  @Test
  @DisplayName("close should fail operations still waiting for a channel")
  @Timeout(5)
  void close_should_fail_operations_waiting_for_a_channel() {
    // Given
    pool = ManagedChannelPool.create(1, this::createHealthyChannel);
    pool.executeAsync(ch -> new CompletableFuture<>());
    var waiting = pool.executeAsync(ch -> CompletableFuture.completedFuture("never")).toCompletableFuture();

    // When
    pool.close();

    // Then
    assertThatThrownBy(() -> waiting.get(2, SECONDS))
      .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("close now should shut every channel down at once and fail operations waiting for one")
  @Timeout(5)
  void close_now_should_shut_channels_down_at_once() {
    // Given: the only channel carries a call that never ends, and another call waits for it
    var channel = createHealthyChannel();
    pool = ManagedChannelPool.create(1, () -> channel);
    pool.executeAsync(ch -> new CompletableFuture<>());
    var waiting = pool.executeAsync(ch -> CompletableFuture.completedFuture("never")).toCompletableFuture();

    // When
    pool.closeNow();

    // Then: no graceful shutdown is attempted, and the pool reports itself closed
    verify(channel).shutdownNow();
    verify(channel, never()).shutdown();
    assertThat(pool.isClosed()).isTrue();
    assertThat(pool.size()).isZero();
    assertThatThrownBy(() -> waiting.get(2, SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
  }

  private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
    var deadline = System.nanoTime() + SECONDS.toNanos(2);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(condition.getAsBoolean()).isTrue();
  }

  @Test
  @DisplayName("pool should reuse the most recently released channel first")
  void pool_should_reuse_the_most_recently_released_channel_first() {
    // Given
    pool = ManagedChannelPool.create(5, this::createHealthyChannel);
    var first = new CompletableFuture<>();
    var second = new CompletableFuture<>();
    var firstChannel = new AtomicReference<ManagedChannel>();
    var secondChannel = new AtomicReference<ManagedChannel>();
    pool.executeAsync(ch -> { firstChannel.set(ch); return first; });
    pool.executeAsync(ch -> { secondChannel.set(ch); return second; });

    // When: the first channel is released, then the second
    first.complete("1");
    second.complete("2");

    // Then: the next call gets the second, released last
    ManagedChannel reused = pool.execute(ch -> ch);
    assertThat(reused).isSameAs(secondChannel.get());
  }

  @Test
  @DisplayName("pool should shut down channels left unused longer than the maximum idle time")
  void pool_should_shut_down_channels_left_unused_too_long() {
    // Given
    var now = new AtomicLong();
    pool = ManagedChannelPool.create(0, this::createHealthyChannel, Duration.ofMinutes(5), now::get);
    var busy = range(0, 3).mapToObj(i -> new CompletableFuture<>()).toList();
    var channels = new CopyOnWriteArrayList<ManagedChannel>();
    busy.forEach(call -> pool.executeAsync(ch -> { channels.add(ch); return call; }));
    busy.forEach(call -> call.complete("done"));
    assertThat(pool.size()).isEqualTo(3);

    // When: one channel keeps serving calls while the others stay unused past the idle time
    now.addAndGet(Duration.ofMinutes(4).toNanos());
    var reused = pool.execute(ch -> ch);
    now.addAndGet(Duration.ofMinutes(2).toNanos());
    pool.execute(ch -> ch);

    // Then: the two unused channels are shut down, the one in use is kept
    assertThat(pool.size()).isEqualTo(1);
    channels.stream().filter(ch -> ch != reused).forEach(ch -> verify(ch).shutdown());
    verify(reused, never()).shutdown();
  }

  @Test
  @DisplayName("execute should return its permit when the channel cannot be created")
  @Timeout(5)
  void execute_should_return_permit_when_channel_creation_fails() throws Exception {
    // Given: a factory that fails twice, then works
    var calls = new AtomicInteger();
    pool = ManagedChannelPool.create(2, () -> {
      if (calls.incrementAndGet() <= 2) throw new IllegalStateException("bad CA path");
      return createHealthyChannel();
    });

    // When
    assertThatThrownBy(() -> pool.execute(ch -> "first")).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> pool.execute(ch -> "second")).isInstanceOf(IllegalStateException.class);

    // Then: both permits are back
    var first = new CompletableFuture<String>();
    var second = new CompletableFuture<String>();
    var started = new AtomicInteger();
    pool.executeAsync(ch -> { started.incrementAndGet(); return first; });
    pool.executeAsync(ch -> { started.incrementAndGet(); return second; });
    assertThat(started.get()).isEqualTo(2);
    first.complete("1");
    second.complete("2");
  }

  @Test
  @DisplayName("an interrupted execute should not lose its permit")
  @Timeout(5)
  void interrupted_execute_should_not_lose_its_permit() throws Exception {
    // Given: both channels of a pool of 2 are busy, and a caller waits in execute()
    pool = ManagedChannelPool.create(2, this::createHealthyChannel);
    var busy = range(0, 2).mapToObj(i -> new CompletableFuture<String>()).toList();
    busy.forEach(call -> pool.executeAsync(ch -> call));
    var waiting = new Thread(() -> {
      try {
        pool.execute(ch -> "never");
      } catch (RuntimeException expected) {
        // interrupted
      }
    });
    waiting.start();
    awaitCondition(() -> waiting.getState() == Thread.State.WAITING || waiting.getState() == Thread.State.TIMED_WAITING);

    // When: the caller gives up, then the channels are released
    waiting.interrupt();
    waiting.join(2000);
    busy.forEach(call -> call.complete("done"));

    // Then: two calls can still run at once
    var started = new AtomicInteger();
    var next = range(0, 2).mapToObj(i -> new CompletableFuture<String>()).toList();
    next.forEach(call -> pool.executeAsync(ch -> { started.incrementAndGet(); return call; }));
    awaitCondition(() -> started.get() == 2);
    next.forEach(call -> call.complete("done"));
  }

  @Test
  @DisplayName("a call cancelled while waiting should neither run nor lose its permit")
  @Timeout(5)
  void cancelled_waiting_call_should_not_run_nor_lose_its_permit() throws Exception {
    // Given: the only channel is busy and a second call waits
    pool = ManagedChannelPool.create(1, this::createHealthyChannel);
    var busy = new CompletableFuture<String>();
    pool.executeAsync(ch -> busy);
    var ranCancelled = new AtomicInteger();
    var cancelled = pool.executeAsync(ch -> { ranCancelled.incrementAndGet(); return CompletableFuture.completedFuture("x"); }).toCompletableFuture();

    // When: the caller gives up, then the channel is released
    cancelled.cancel(false);
    busy.complete("done");

    // Then: the cancelled call never runs, and the next one does
    var next = pool.executeAsync(ch -> CompletableFuture.completedFuture("next")).toCompletableFuture();
    assertThat(next.get(2, SECONDS)).isEqualTo("next");
    assertThat(ranCancelled.get()).isZero();
  }

  @Test
  @DisplayName("a call handed a permit while the pool closes should not leave an open channel")
  @Timeout(10)
  void call_handed_a_permit_during_close_should_not_leave_an_open_channel() throws Exception {
    for (int attempt = 0; attempt < 50; attempt++) {
      // Given: the only channel is busy and a second call waits
      var created = new CopyOnWriteArrayList<ManagedChannel>();
      pool = ManagedChannelPool.create(1, () -> {
        var channel = createHealthyChannel();
        created.add(channel);
        return channel;
      });
      var busy = new CompletableFuture<String>();
      pool.executeAsync(ch -> busy);
      var ran = new AtomicInteger();
      var waiting = pool.executeAsync(ch -> { ran.incrementAndGet(); return CompletableFuture.completedFuture("x"); }).toCompletableFuture();

      // When: the channel is released and the pool closes right away
      busy.complete("done");
      pool.close();

      // Then: the waiting call either ran before the close or failed without running,
      // and every channel the pool created is shut down
      var outcome = waiting.handle((value, error) -> error == null).get(2, SECONDS);
      assertThat(ran.get()).isEqualTo(outcome ? 1 : 0);
      created.forEach(channel -> verify(channel, atLeastOnce()).shutdown());
    }
  }

  private static Function<ManagedChannel, CompletionStage<Object>> executeOperation(CountDownLatch acquired, CompletableFuture<Object> operation1) {
    return channel -> {
      acquired.countDown();
      return operation1;
    };
  }

  private ManagedChannel createHealthyChannel() {
    var channel = mock(ManagedChannel.class);
    when(channel.getState(false)).thenReturn(READY);
    when(channel.shutdown()).thenReturn(channel);
    try {
      when(channel.awaitTermination(anyLong(), any())).thenReturn(true);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return channel;
  }

  private ManagedChannel createUnhealthyChannel() {
    var channel = mock(ManagedChannel.class);
    when(channel.getState(false)).thenReturn(TRANSIENT_FAILURE);
    when(channel.shutdown()).thenReturn(channel);
    try {
      when(channel.awaitTermination(anyLong(), any())).thenReturn(true);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return channel;
  }
}
