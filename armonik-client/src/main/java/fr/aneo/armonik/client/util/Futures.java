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
package fr.aneo.armonik.client.util;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.google.common.util.concurrent.Futures.addCallback;
import static fr.aneo.armonik.client.util.ExecutorProvider.defaultExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

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
   * Returns a {@link CompletionStage} that completes when all the given stages complete.
   *
   * <p>The returned stage:</p>
   * <ul>
   *   <li>Completes successfully with a {@link List} of results in the same order
   *       as the input stages, if all succeed.</li>
   *   <li>Completes exceptionally if <em>any</em> stage fails. The exception from one
   *       failed stage is propagated (others are suppressed by default).</li>
   * </ul>
   *
   * @param stages the stages to combine
   * @param <T>    the result type
   * @return a stage that yields a list of results or fails if any input stage fails
   * @throws NullPointerException if {@code stages} is null
   */
  public static <T> CompletionStage<List<T>> allOf(Collection<CompletionStage<T>> stages) {
    if (stages.isEmpty()) return completedFuture(List.of());

    var futures = stages.stream().map(CompletionStage::toCompletableFuture).toList();

    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                            .thenApply(v -> futures.stream()
                                                   // allOf ensures all futures are completed, so join() will not block here.
                                                   .map(CompletableFuture::join)
                                                   .toList());
  }


  /**
   * Returns a {@link CompletionStage} that completes when all the given keyed stages complete.
   *
   * <p>The returned stage:
   * <ul>
   *   <li>Completes successfully with a {@link Map} from each key to its resolved value,
   *       preserving the iteration order of the input map, if all succeed.</li>
   *   <li>Completes exceptionally if <em>any</em> stage fails. The exception from one
   *       failed stage is propagated (others are suppressed by default).</li>
   * </ul>
   *
   * <p>Note: The returned map is a {@link LinkedHashMap}, ensuring deterministic ordering
   * that follows the iteration order of the input map.
   *
   * @param stages the map of keys to completion stages; must not be {@code null}
   * @param <K>    the type of keys in the input and result map
   * @param <V>    the result type of each stage
   * @return a stage that yields a map of completed results or fails if any input stage fails
   * @throws NullPointerException if {@code stages} is null
   */
  public static <K, V> CompletionStage<Map<K, V>> allOf(Map<K, CompletionStage<V>> stages) {
    if (stages.isEmpty()) return completedFuture(Map.of());

    List<CompletableFuture<?>> futures = stages.values()
                                               .stream()
                                               .map(CompletionStage::toCompletableFuture)
                                               .collect(toList());

    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                            .thenApply(v -> {
                              var result = new LinkedHashMap<K, V>(stages.size());
                              // allOf ensures all futures are completed, so join() will not block here.
                              stages.forEach((k, stg) -> result.put(k, stg.toCompletableFuture().join()));
                              return result;
                            });
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
