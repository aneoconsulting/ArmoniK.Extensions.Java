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

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
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

    IntStream.range(0, 3).<Callable<String>>mapToObj(i -> () -> pool.execute(ch -> {
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
    IntStream.range(0, 10).forEach(i -> pool.execute(ch -> "operation-" + i));

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

  private ManagedChannel createHealthyChannel() {
    var channel = mock(ManagedChannel.class);
    when(channel.getState(false)).thenReturn(ConnectivityState.READY);
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
    when(channel.getState(false)).thenReturn(ConnectivityState.TRANSIENT_FAILURE);
    when(channel.shutdown()).thenReturn(channel);
    try {
      when(channel.awaitTermination(anyLong(), any())).thenReturn(true);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return channel;
  }
}
