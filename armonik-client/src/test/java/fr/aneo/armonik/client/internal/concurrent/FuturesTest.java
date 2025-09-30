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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FuturesTest {

  @Test
  void allOf_of_empty_list_returns_completed_with_empty_list() {
    // When
    var combined = Futures.allOf(List.of()).toCompletableFuture().join();

    // Then
    assertThat(combined).isEmpty();
  }

  @Test
  void allOf_list_preserves_order() {
    // Given
    List<CompletionStage<String>> stages = List.of(
      completedFuture("A"),
      completedFuture("B"),
      completedFuture("C")
    );

    // When
    var combined = Futures.allOf(stages).toCompletableFuture().join();

    // Then
    assertThat(combined).containsExactly("A", "B", "C");
  }

  @Test
  void allOf_list_fails_when_any_fails() {
    // Given
    List<CompletionStage<String>> stages = List.of(
      completedFuture("A"),
      failedFuture(new IllegalStateException("boom")),
      completedFuture("B")
    );

    // When
    var combined = Futures.allOf(stages).toCompletableFuture();

    // Then
    assertThatThrownBy(combined::get)
      .isInstanceOf(ExecutionException.class)
      .hasCauseInstanceOf(IllegalStateException.class)
      .hasRootCauseMessage("boom");
  }


  @Test
  void allOf_of_empty_map_returns_completed_with_empty_map() {
    // When
    var combined = Futures.allOf(Map.<String, CompletionStage<Integer>>of()).toCompletableFuture().join();

    // Then
    assertThat(combined).isEmpty();
  }

  @Test
  void allOf_map_preserves_iteration_order() {
    // Given
    var stages = new LinkedHashMap<String, CompletionStage<String>>();
    stages.put("k1", completedFuture("A"));
    stages.put("k2", completedFuture("B"));
    stages.put("k3", completedFuture("C"));

    // When
    var combined = Futures.allOf(stages).toCompletableFuture().join();

    // Then
    assertThat(combined).containsExactly(
      Map.entry("k1", "A"),
      Map.entry("k2", "B"),
      Map.entry("k3", "C")
    );
  }

  @Test
  void allOf_map_fails_when_any_fails() {
    // Given
    var stages = new LinkedHashMap<String, CompletionStage<String>>();
    stages.put("ok", completedFuture("A"));
    stages.put("bad", failedFuture(new IllegalStateException("boom")));
    stages.put("later", completedFuture("B"));

    // When
    var combined = Futures.allOf(stages).toCompletableFuture();

    // Then
    assertThatThrownBy(combined::get)
      .isInstanceOf(ExecutionException.class)
      .hasCauseInstanceOf(IllegalStateException.class)
      .hasRootCauseMessage("boom");
  }
}
