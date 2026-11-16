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
package fr.aneo.armonik.client.internal.concurrent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static fr.aneo.armonik.client.internal.concurrent.ExecutorProvider.defaultExecutor;

/**
 * Counting limiter whose waiters are futures instead of blocked threads.
 * <p>
 * A released permit goes to the oldest waiter, which is completed on the client executor so the
 * releasing thread, often a gRPC callback thread, never runs the waiter's continuation. A waiter
 * that is cancelled before it gets its permit, because its caller was interrupted or gave up,
 * passes the permit on, so no permit is ever lost. Once closed, the limiter fails every new waiter.
 */
public final class AsyncLimiter {
  private final int max;
  private final Deque<CompletableFuture<Void>> waiters = new ArrayDeque<>();
  private int inUse;
  private Throwable closedWith;

  public AsyncLimiter(int max) {
    this.max = max;
  }

  /**
   * Requests a permit.
   *
   * @return a future completing once the permit is granted, completed at once when one is free, or
   * failed when the limiter is closed
   */
  public CompletableFuture<Void> acquire() {
    synchronized (this) {
      if (closedWith != null) return CompletableFuture.failedFuture(closedWith);
      if (inUse < max) {
        inUse++;
        return CompletableFuture.completedFuture(null);
      }
      var waiter = new CompletableFuture<Void>();
      waiters.add(waiter);
      return waiter;
    }
  }

  /**
   * Returns a permit, handing it to the oldest waiter if there is one.
   */
  public void release() {
    CompletableFuture<Void> next;
    synchronized (this) {
      next = waiters.poll();
      if (next == null) {
        inUse--;
        return;
      }
    }
    defaultExecutor().execute(() -> {
      if (!next.complete(null)) {
        // The waiter was cancelled after it left the queue: pass the permit on.
        release();
      }
    });
  }

  /**
   * Gives up a waiter returned by {@link #acquire()}.
   *
   * @param waiter the waiter to give up
   * @return true when the waiter was cancelled and holds no permit, false when its permit was
   * already granted, in which case the caller must {@link #release()} it
   */
  public boolean cancel(CompletableFuture<Void> waiter) {
    if (waiter.cancel(false)) {
      synchronized (this) {
        waiters.remove(waiter);
      }
      return true;
    }
    return waiter.isCompletedExceptionally();
  }

  /**
   * Runs an operation once a permit is granted, and returns the permit when the operation's stage completes.
   * <p>
   * Completing or cancelling the returned future before the permit is granted gives up the wait, and
   * the operation never runs.
   *
   * @param operation the operation to run with a permit
   * @param <T>       the type of the operation's result
   * @return a future completing with the operation's result
   */
  public <T> CompletableFuture<T> withPermit(Supplier<? extends CompletionStage<T>> operation) {
    var result = new CompletableFuture<T>();
    var waiter = acquire();
    result.whenComplete((value, error) -> {
      if (!waiter.isDone()) cancel(waiter);
    });
    waiter.whenComplete((ignored, error) -> {
      if (error != null) {
        result.completeExceptionally(error);
        return;
      }
      if (result.isDone()) {
        release();
        return;
      }
      CompletionStage<T> stage;
      try {
        stage = operation.get();
      } catch (Throwable throwable) {
        release();
        result.completeExceptionally(throwable);
        return;
      }
      stage.whenComplete((value, failure) -> {
        release();
        if (failure != null) result.completeExceptionally(failure);
        else result.complete(value);
      });
    });
    return result;
  }

  /**
   * Fails the waiters still queued and every later {@link #acquire()}.
   *
   * @param error the error to fail them with
   */
  public void close(Throwable error) {
    List<CompletableFuture<Void>> failed;
    synchronized (this) {
      closedWith = error;
      failed = List.copyOf(waiters);
      waiters.clear();
    }
    failed.forEach(waiter -> waiter.completeExceptionally(error));
  }
}
