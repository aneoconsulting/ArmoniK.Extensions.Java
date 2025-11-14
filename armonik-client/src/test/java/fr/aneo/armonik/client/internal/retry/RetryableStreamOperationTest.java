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
package fr.aneo.armonik.client.internal.retry;

import fr.aneo.armonik.client.RetryPolicy;
import fr.aneo.armonik.client.exception.ArmoniKException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.*;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryableStreamOperationTest {

  private static final RetryPolicy FAST_RETRY_POLICY = new RetryPolicy(
    3,
    Duration.ofMillis(10),
    Duration.ofMillis(50),
    1.5
  );
  private Supplier<CompletionStage<Object>> operation;

  @Test
  @DisplayName("execute completes successfully on first attempt")
  void execute_completes_successfully_on_first_attempt() {
    // Given
    operation = () -> completedFuture("success");

    // When
    var result = RetryableStreamOperation.execute(operation, FAST_RETRY_POLICY).toCompletableFuture().join();

    // Then
    assertThat(result).isEqualTo("success");
  }

  @ParameterizedTest
  @EnumSource(value = Status.Code.class, names = {"UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "ABORTED"})
  @DisplayName("execute retries on retryable status codes and succeeds")
  void execute_retries_on_retryable_status_and_succeeds(Status.Code statusCode) {
    // Given
    var attemptCount = new AtomicInteger(0);
    operation = () -> {
      if (attemptCount.incrementAndGet() < 2) {
        return CompletableFuture.failedFuture(new StatusRuntimeException(Status.fromCode(statusCode)));
      }
      return CompletableFuture.completedFuture("success");
    };

    // When
    var result = RetryableStreamOperation.execute(operation, FAST_RETRY_POLICY).toCompletableFuture().join();

    // Then
    assertThat(result).isEqualTo("success");
    assertThat(attemptCount.get()).isEqualTo(2);
  }

  @ParameterizedTest
  @EnumSource(value = Status.Code.class, names = {"INVALID_ARGUMENT", "NOT_FOUND", "PERMISSION_DENIED", "UNAUTHENTICATED"})
  @DisplayName("execute fails immediately on non-retryable status codes without retry")
  void execute_fails_immediately_on_non_retryable_status_without_retry(Status.Code statusCode) {
    // Given
    var attemptCount = new AtomicInteger(0);
    operation = () -> {
      attemptCount.incrementAndGet();
      return CompletableFuture.failedFuture(new StatusRuntimeException(Status.fromCode(statusCode)));
    };

    // When/Then
    assertThatThrownBy(() -> RetryableStreamOperation.execute(operation, FAST_RETRY_POLICY).toCompletableFuture().join())
      .isInstanceOf(CompletionException.class)
      .hasCauseInstanceOf(StatusRuntimeException.class);

    assertThat(attemptCount.get()).isEqualTo(1);
  }

  @Test
  @DisplayName("execute fails after exhausting max attempts")
  void execute_fails_after_exhausting_max_attempts() {
    // Given
    var attemptCount = new AtomicInteger(0);
    operation = () -> {
      attemptCount.incrementAndGet();
      return failedFuture(new StatusRuntimeException(Status.UNAVAILABLE));
    };

    // When/Then
    assertThatThrownBy(() -> RetryableStreamOperation.execute(operation, FAST_RETRY_POLICY).toCompletableFuture().join())
      .isInstanceOf(CompletionException.class)
      .hasCauseInstanceOf(StatusRuntimeException.class);

    assertThat(attemptCount.get()).isEqualTo(3);
  }

  @Test
  @DisplayName("execute unwraps CompletionException to find StatusRuntimeException")
  void execute_unwraps_CompletionException_to_find_StatusRuntimeException() {
    // Given
    var attemptCount = new AtomicInteger(0);
    var statusException = new StatusRuntimeException(Status.UNAVAILABLE);
    var wrappedException = new CompletionException(statusException);
    operation = () -> {
      if (attemptCount.incrementAndGet() < 2) return failedFuture(wrappedException);
      return completedFuture("success");
    };

    // When
    var result = RetryableStreamOperation.execute(operation, FAST_RETRY_POLICY).toCompletableFuture().join();

    // Then
    assertThat(result).isEqualTo("success");
    assertThat(attemptCount.get()).isEqualTo(2);
  }

  @Test
  @DisplayName("execute unwraps ArmoniKException to find StatusRuntimeException")
  void execute_unwraps_ArmoniKException_to_find_StatusRuntimeException() {
    // Given
    var attemptCount = new AtomicInteger(0);
    var statusException = new StatusRuntimeException(Status.UNAVAILABLE);
    var armoniKException = new ArmoniKException("wrapped", statusException);
    operation = () -> {
      if (attemptCount.incrementAndGet() < 2) return failedFuture(armoniKException);
      return completedFuture("success");
    };

    // When
    var result = RetryableStreamOperation.execute(operation, FAST_RETRY_POLICY).toCompletableFuture().join();

    // Then
    assertThat(result).isEqualTo("success");
    assertThat(attemptCount.get()).isEqualTo(2);
  }


  @Test
  @DisplayName("execute handles synchronous exceptions from operation supplier")
  void execute_handles_synchronous_exceptions_from_operation_supplier() {
    // Given
    operation = () -> {
      throw new IllegalStateException("sync error");
    };

    // When/Then
    assertThatThrownBy(() -> RetryableStreamOperation.execute(operation, FAST_RETRY_POLICY).toCompletableFuture().join())
      .isInstanceOf(CompletionException.class)
      .hasCauseInstanceOf(IllegalStateException.class);
  }
}
