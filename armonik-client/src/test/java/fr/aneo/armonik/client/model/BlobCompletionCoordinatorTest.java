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

import fr.aneo.armonik.client.testutils.CountingDeterministicScheduler;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static fr.aneo.armonik.client.model.TestDataFactory.blobHandle;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class BlobCompletionCoordinatorTest {

  private CountingDeterministicScheduler scheduler;
  private DeterministicScheduler completionScheduler;
  private BlobCompletionEventWatcher watcher;
  private AtomicInteger completionSeq;

  @BeforeEach
  void setUp() {
    watcher = mock(BlobCompletionEventWatcher.class);
    scheduler = new CountingDeterministicScheduler();
    completionScheduler = new CountingDeterministicScheduler();
    completionSeq = new AtomicInteger(0);
    when(watcher.watch(anyList())).thenAnswer(inv -> {
      var f = new CompletableFuture<Void>();
      int delaySeconds = completionSeq.incrementAndGet();
      completionScheduler.schedule(() -> { f.complete(null); }, delaySeconds, SECONDS);
      return f;
    });
  }

  @Test
  void should_flush_immediately_when_batch_size_threshold_is_reached() {
    // Given
    var policy = new BatchingPolicy(3, ofSeconds(10), 2, 100);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    coordinator.enqueue(blobHandles(3));

    // Then
    verify(watcher).watch(argThat(handles -> handles.size() == 3));

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_flush_when_timer_expires_and_batch_size_not_reached() {
    // Given
    var policy = new BatchingPolicy(10, ofSeconds(1), 2, 100);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    coordinator.enqueue(blobHandles(2));

    // Then
    verify(watcher, never()).watch(anyList());

    // And
    scheduler.tick(1, SECONDS);
    verify(watcher).watch(argThat(handles -> handles.size() == 2));
    assertThat(scheduler.scheduleCount()).isEqualTo(1);

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_aggregate_multiple_enqueues_into_single_batch_when_timer_fires() {
    // Given
    var policy = new BatchingPolicy(10, ofSeconds(1), 2, 100);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    coordinator.enqueue(blobHandles(1));
    coordinator.enqueue(blobHandles(1));

    // Then
    verify(watcher, never()).watch(anyList());

    // And
    scheduler.tick(1, SECONDS);
    verify(watcher).watch(argThat(handles -> handles.size() == 2));
    assertThat(scheduler.scheduleCount()).isEqualTo(1);

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_cancel_timer_after_size_triggered_flush_and_not_fire_later() {
    // Given
    var policy = new BatchingPolicy(3, ofSeconds(1), 2, 100);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    coordinator.enqueue(blobHandles(3));

    // Then
    verify(watcher).watch(argThat(handles -> handles.size() == 3));

    // And
    scheduler.tick(1, SECONDS);
    verify(watcher).watch(anyList());

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_rearm_timer_for_new_epoch_after_flush() {
    // Given
    var policy = new BatchingPolicy(10, ofSeconds(1), 2, 100);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    coordinator.enqueue(blobHandles(2));
    assertThat(scheduler.scheduleCount()).isEqualTo(1);
    scheduler.tick(1, SECONDS);

    // Then
    verify(watcher).watch(argThat(handles -> handles.size() == 2));

    // And
    coordinator.enqueue(blobHandles(1));
    assertThat(scheduler.scheduleCount()).isEqualTo(2);
    scheduler.tick(1, SECONDS);
    verify(watcher).watch(argThat(handles -> handles.size() == 1));

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_split_batch_by_cap_on_size_triggered_flush() {
    // Given
    var policy = new BatchingPolicy(1, ofSeconds(10), 2, 3);
    when(watcher.watch(anyList())).thenReturn(completedFuture(null));
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    coordinator.enqueue(blobHandles(7));

    // Then
    var captor = blobHandlesCaptor();
    verify(watcher, times(3)).watch(captor.capture());

    var sizes = captor.getAllValues().stream().map(List::size).toList();
    assertThat(sizes).containsExactly(3, 3, 1);

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_split_batch_by_cap_on_timer_flush() {
    // Given
    var policy = new BatchingPolicy(1000, ofSeconds(1), 2, 3);
    when(watcher.watch(anyList())).thenReturn(completedFuture(null));
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    coordinator.enqueue(blobHandles(7));
    verify(watcher, never()).watch(anyList());
    scheduler.tick(1, SECONDS);

    // Then
    var captor = blobHandlesCaptor();
    verify(watcher, times(3)).watch(captor.capture());

    var sizes = captor.getAllValues().stream().map(List::size).toList();
    assertThat(sizes).containsExactly(3, 3, 1);
    assertThat(scheduler.scheduleCount()).isEqualTo(1);

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_not_exceed_max_concurrent_batches() {
    // Given
    var policy = new BatchingPolicy(1, ofSeconds(10), 2, 100);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    coordinator.enqueue(blobHandles(1));
    coordinator.enqueue(blobHandles(1));
    coordinator.enqueue(blobHandles(1));

    // Then
    verify(watcher, times(2)).watch(anyList());

    // And when: frees one slot
    completionScheduler.tick(1, SECONDS);

    // Then: third call should start
    verify(watcher, times(3)).watch(anyList());

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_accumulate_while_saturated_and_flush_on_completion_respecting_cap() {
    // Given
    var policy = new BatchingPolicy(1, ofSeconds(10), 2, 3);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    coordinator.enqueue(blobHandles(1));
    coordinator.enqueue(blobHandles(1));

    // When
    coordinator.enqueue(blobHandles(5));

    // Then
    verify(watcher, times(2)).watch(anyList());

    // And when: complete one running call to free a slot
    completionScheduler.tick(1, SECONDS);

    // Then
    var captor = blobHandlesCaptor();
    verify(watcher, times(3)).watch(captor.capture());
    assertThat(captor.getValue().size()).isEqualTo(3);

    // And when: completes the second running call
    completionScheduler.tick(1, SECONDS);

    // Then: remaining 2 should flush in one more call
    verify(watcher, times(4)).watch(captor.capture());
    assertThat(captor.getValue().size()).isEqualTo(2);

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void should_not_dispatch_on_timer_while_saturated() {
    // Given
    var policy = new BatchingPolicy(1, ofSeconds(1), 2, 3);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    coordinator.enqueue(blobHandles(1));
    coordinator.enqueue(blobHandles(1));

    // When
    coordinator.enqueue(blobHandles(5));

    // Then: still only 2 calls running so far
    verify(watcher, times(2)).watch(anyList());

    // And When: even if the timer expires while saturated
    scheduler.tick(1, SECONDS);

    // Then: no additional dispatch should occur due to saturation
    verify(watcher, times(2)).watch(anyList());

    // And When: a slot frees up
    completionScheduler.tick(1, SECONDS);

    // Then: exactly one new call starts
    var captor = blobHandlesCaptor();
    verify(watcher, times(3)).watch(captor.capture());
    assertThat(captor.getValue().size()).isEqualTo(3);

    // Cleanup
    completeAllAndAwait(coordinator);
  }

  @Test
  void waitUntilIdle_should_drain_buffer_and_inflight() {
    // Given
    var policy = new BatchingPolicy(1, ofSeconds(10), 1, 3);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);
    coordinator.enqueue(blobHandles(7));

    // When
    var idle = coordinator.waitUntilIdle();
    completionScheduler.tick(1, DAYS);
    completionScheduler.runUntilIdle();

    // Then
    assertThat(idle.toCompletableFuture()).isCompleted();
  }

  @Test
  void waitUntilIdle_should_complete_successfully_even_if_some_batches_fail() {
    // Given
    var policy = new BatchingPolicy(1, ofSeconds(10), 1, 3);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);
    var callIdx = new AtomicInteger(0);
    when(watcher.watch(anyList())).thenAnswer(inv -> {
      var f = new CompletableFuture<Void>();
      int i = callIdx.getAndIncrement();
      if (i == 0) {
        completionScheduler.schedule(() -> f.completeExceptionally(new RuntimeException("boom")), 1, SECONDS);
      } else {
        completionScheduler.schedule(() -> f.complete(null), 2, SECONDS);
      }
      return f;
    });
    coordinator.enqueue(blobHandles(4));

    // When
    var idle = coordinator.waitUntilIdle();
    completionScheduler.tick(1, DAYS);
    completionScheduler.runUntilIdle();

    // Then
    assertThat(idle.toCompletableFuture()).isCompleted();
  }

  @Test
  void waitUntilIdle_should_return_immediately_when_no_work() {
    // Given
    var policy = new BatchingPolicy(1, ofSeconds(10), 1, 3);
    var coordinator = new BlobCompletionCoordinator(watcher, policy, scheduler);

    // When
    var idle = coordinator.waitUntilIdle();

    // Then
    assertThat(idle.toCompletableFuture()).isCompleted();
  }

  @SuppressWarnings("unchecked")
  private static ArgumentCaptor<List<BlobHandle>> blobHandlesCaptor() {
    return ArgumentCaptor.forClass(List.class);
  }

  private static List<BlobHandle> blobHandles(int n) {
    return IntStream.range(0, n)
                    .mapToObj(i -> blobHandle("sessionId", "blobId-" + i))
                    .toList();
  }

  private void completeAllAndAwait(BlobCompletionCoordinator coordinator) {
    completionScheduler.tick(1, DAYS);
    completionScheduler.runUntilIdle();
    coordinator.waitUntilIdle().toCompletableFuture().join();
  }
}
