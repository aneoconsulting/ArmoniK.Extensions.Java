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

import com.google.common.util.concurrent.SettableFuture;
import fr.aneo.armonik.client.exception.ArmoniKException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FuturesTest {

  @Test
  @DisplayName("allOf of empty list returns completed with empty list")
  void allOf_of_empty_list_returns_completed_with_empty_list() {
    // When
    var combined = Futures.allOf(List.of()).toCompletableFuture().join();

    // Then
    assertThat(combined).isEmpty();
  }

  @Test
  @DisplayName("allOf list preserves order")
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
  @DisplayName("allOf list fails when any fails")
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
  @DisplayName("allOf of empty map returns completed with empty map")
  void allOf_of_empty_map_returns_completed_with_empty_map() {
    // When
    var combined = Futures.allOf(Map.<String, CompletionStage<Integer>>of()).toCompletableFuture().join();

    // Then
    assertThat(combined).isEmpty();
  }

  @Test
  @DisplayName("allOf map preserves iteration order")
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
  @DisplayName("allOf map fails when any fails")
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

  @Test
  @DisplayName("toCompletionStage completes successfully when ListenableFuture succeeds")
  void toCompletionStage_completes_successfully_when_ListenableFuture_succeeds() {
    // Given
    var listenableFuture = SettableFuture.<String>create();

    // When
    var completionStage = Futures.toCompletionStage(listenableFuture);
    listenableFuture.set("success");

    // Then
    assertThat(completionStage.toCompletableFuture().join()).isEqualTo("success");
  }

  @Test
  @DisplayName("toCompletionStage wraps exception in ArmoniKException")
  void toCompletionStage_wraps_exception_in_ArmoniKException() {
    // Given
    var listenableFuture = SettableFuture.<String>create();
    var originalException = new IllegalStateException("boom");

    // When
    var completionStage = Futures.toCompletionStage(listenableFuture);
    listenableFuture.setException(originalException);

    // Then
    assertThatThrownBy(() -> completionStage.toCompletableFuture().join())
      .isInstanceOf(CompletionException.class)
      .hasCauseInstanceOf(ArmoniKException.class)
      .hasRootCause(originalException)
      .hasMessageContaining("boom");
  }

  @Test
  @DisplayName("toCompletionStage cancels when ListenableFuture throws CancellationException")
  void toCompletionStage_cancels_when_ListenableFuture_throws_CancellationException() {
    // Given
    var listenableFuture = SettableFuture.<String>create();

    // When
    var completionStage = Futures.toCompletionStage(listenableFuture);
    listenableFuture.setException(new CancellationException("cancelled"));

    // Then
    assertThatThrownBy(() -> completionStage.toCompletableFuture().join())
      .isInstanceOf(CancellationException.class);
  }

  @Test
  @DisplayName("toCompletionStage uses exception message when available")
  void toCompletionStage_uses_exception_message_when_available() {
    // Given
    var listenableFuture = SettableFuture.<String>create();
    var exception = new RuntimeException("detailed error message");

    // When
    var completionStage = Futures.toCompletionStage(listenableFuture);
    listenableFuture.setException(exception);

    // Then
    assertThatThrownBy(() -> completionStage.toCompletableFuture().join())
      .isInstanceOf(CompletionException.class)
      .cause()
      .isInstanceOf(ArmoniKException.class)
      .hasMessage("detailed error message")
      .hasCause(exception);
  }

  @Test
  @DisplayName("toCompletionStage uses exception class name when message is null")
  void toCompletionStage_uses_exception_class_name_when_message_is_null() {
    // Given
    var listenableFuture = SettableFuture.<String>create();
    var exception = new NullPointerException();

    // When
    var completionStage = Futures.toCompletionStage(listenableFuture);
    listenableFuture.setException(exception);

    // Then
    assertThatThrownBy(() -> completionStage.toCompletableFuture().join())
      .isInstanceOf(CompletionException.class)
      .cause()
      .isInstanceOf(ArmoniKException.class)
      .hasMessage("NullPointerException")
      .hasCause(exception);
  }
}
