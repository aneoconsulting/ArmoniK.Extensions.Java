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
package fr.aneo.armonik.worker.domain.internal.concurrent;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;


/**
 * Utility class providing static methods for combining or synchronizing multiple {@link CompletionStage} instances.
 * <p>
 * This class simplifies concurrent composition patterns and ensures predictable behavior for both collections and maps
 * of futures. It allows waiting for all asynchronous operations to complete and aggregates their results, propagating
 * exceptions if any stage fails.
 * </p>
 *
 */
public final class Futures {
  private Futures() {
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
}
