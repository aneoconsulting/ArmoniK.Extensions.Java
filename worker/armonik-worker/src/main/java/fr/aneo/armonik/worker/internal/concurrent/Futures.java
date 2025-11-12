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
package fr.aneo.armonik.worker.internal.concurrent;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.google.common.util.concurrent.Futures.addCallback;
import static fr.aneo.armonik.worker.internal.concurrent.ExecutorProvider.defaultExecutor;
import static java.util.Objects.requireNonNull;

/**
 * Utilities to bridge Guava {@link ListenableFuture} to Java {@link CompletionStage}.
 * <p>
 * The adapter preserves completion semantics and propagates cancellation from the resulting
 * {@link CompletableFuture} back to the source {@link ListenableFuture}. Callbacks are invoked
 * on the client's shared executor provided by {@link ExecutorProvider#defaultExecutor()}.
 * </p>
 *
 */

public final class Futures {
  private Futures() {
  }

  /**
   * Adapts a Guava {@link ListenableFuture} to a Java {@link CompletionStage}.
   * Behavior:
   * <ul>
   *   <li>On success, completes the resulting {@link CompletionStage} with the value.</li>
   *   <li>On failure, completes exceptionally with the original throwable, unless it is a
   *       {@link CancellationException}, in which case the result is cancelled.</li>
   *   <li>If the returned {@code CompletionStage} is cancelled, the source {@code ListenableFuture}
   *       is also cancelled (with interruption allowed).</li>
   * </ul>
   * Callbacks are scheduled on the shared executor.
   *
   * @param listenableFuture the source future to adapt; must not be {@code null}
   * @param <T>              the result type
   * @return a {@link CompletionStage} reflecting the source future's outcome
   * @throws NullPointerException if {@code listenableFuture} is {@code null}
   */
  public static <T> CompletionStage<T> toCompletionStage(ListenableFuture<T> listenableFuture) {
    requireNonNull(listenableFuture);

    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    addCallback(listenableFuture, completingCallback(completableFuture), defaultExecutor());

    completableFuture.whenComplete((value, throwable) -> {
      if (completableFuture.isCancelled()) listenableFuture.cancel(true);
    });

    return completableFuture;
  }


  /**
   * Creates a Guava {@link FutureCallback} that completes the provided {@link CompletableFuture}
   * according to the source future's outcome.
   */
  private static <T> FutureCallback<T> completingCallback(CompletableFuture<T> completableFuture) {
    return new FutureCallback<>() {
      @Override
      public void onSuccess(T result) {
        completableFuture.complete(result);
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (throwable instanceof CancellationException) {
          completableFuture.cancel(false);
        } else {
          completableFuture.completeExceptionally(throwable);
        }
      }
    };
  }
}
