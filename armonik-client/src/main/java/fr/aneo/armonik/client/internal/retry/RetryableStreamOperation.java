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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.delayedExecutor;

/**
 * Utility for executing streaming operations with exponential backoff retry logic.
 * <p>
 * Provides automatic retry for transient failures in gRPC streaming operations
 * such as blob uploads and downloads. Retries are attempted with exponential
 * backoff according to the configured {@link RetryPolicy}.
 * <p>
 * This utility is designed for application-level retries where the entire
 * streaming operation is retried on failure, as opposed to channel-level
 * retries which handle in-flight network errors.
 * <p>
 * Only specific gRPC status codes that indicate transient failures are retried:
 * UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, and ABORTED. Other errors
 * fail immediately without retry attempts.
 *
 * @see RetryPolicy
 */
public final class RetryableStreamOperation {
  private static final Logger log = LoggerFactory.getLogger(RetryableStreamOperation.class);

  private RetryableStreamOperation() {
  }

  /**
   * Executes a streaming operation with automatic retry on transient gRPC failures.
   * <p>
   * The operation will be retried according to the retry policy if it fails with
   * a retryable gRPC status code (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED).
   * Each retry is preceded by an exponential backoff delay.
   * <p>
   * The backoff delay starts with {@link RetryPolicy#initialBackoff()} and increases
   * exponentially by {@link RetryPolicy#backoffMultiplier()} for each subsequent attempt,
   * up to {@link RetryPolicy#maxBackoff()}.
   * <p>
   * If all retry attempts are exhausted or a non-retryable error occurs, the completion
   * stage completes exceptionally with the original error (unwrapped from CompletionException
   * if applicable).
   *
   * @param operation supplier that executes the streaming operation and returns a completion stage
   * @param policy    retry policy specifying max attempts and backoff parameters
   * @param <T>       the type of the operation result
   * @return a completion stage that completes with the operation result on success,
   * or completes exceptionally if all retries fail or a non-retryable error occurs
   * @throws NullPointerException if operation or policy is null
   */
  public static <T> CompletionStage<T> execute(Supplier<CompletionStage<T>> operation, RetryPolicy policy) {
    requireNonNull(operation, "operation must not be null");
    requireNonNull(policy, "policy must not be null");

    return executeAttempt(operation, policy, 1);
  }

  /**
   * Determines if a throwable represents a retryable gRPC error.
   * <p>
   * The following status codes are considered retryable:
   * <ul>
   *   <li><strong>UNAVAILABLE</strong> - Service temporarily unavailable</li>
   *   <li><strong>DEADLINE_EXCEEDED</strong> - Request timeout</li>
   *   <li><strong>RESOURCE_EXHAUSTED</strong> - Rate limiting or quota exceeded</li>
   *   <li><strong>ABORTED</strong> - Operation aborted (e.g., transaction conflicts)</li>
   * </ul>
   * <p>
   * Non-retryable errors (e.g., INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED)
   * will return false, causing the operation to fail immediately.
   * <p>
   * This classification aligns with gRPC's standard retry policy for transient failures.
   *
   * @param throwable the throwable to classify
   * @return true if the error is retryable, false otherwise
   */
  private static boolean isRetryable(Throwable throwable) {
    if (!(throwable instanceof StatusRuntimeException statusRuntimeException)) return false;

    var code = statusRuntimeException.getStatus().getCode();
    return code == Status.Code.UNAVAILABLE
      || code == Status.Code.DEADLINE_EXCEEDED
      || code == Status.Code.RESOURCE_EXHAUSTED
      || code == Status.Code.ABORTED;
  }

  /**
   * Recursively executes a retry attempt.
   * <p>
   * On failure, determines if the error is retryable and if retry attempts remain,
   * then schedules a delayed retry with exponential backoff.
   *
   * @param operation supplier that executes the operation
   * @param policy    retry policy configuration
   * @param attempt   current attempt number (1-based)
   * @param <T>       result type
   * @return completion stage with the operation result or error
   */
  private static <T> CompletionStage<T> executeAttempt(Supplier<CompletionStage<T>> operation, RetryPolicy policy, int attempt) {
    try {
      return operation.get()
                      .exceptionallyCompose(throwable -> {
                        var cause = unwrapException(throwable);
                        if (!shouldRetry(cause, attempt, policy)) {
                          return CompletableFuture.failedFuture(cause);
                        }
                        Duration backoff = calculateBackoff(policy, attempt);
                        log.warn("Streaming operation failed({}), retrying after backoff. Attempt {}/{}, backoff {}",
                          cause.getMessage(), attempt, policy.maxAttempts(), backoff.toMillis(), cause);

                        return delayedRetry(operation, policy, attempt + 1, backoff);
                      });
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Schedules a delayed retry after the specified backoff period.
   *
   * @param operation   supplier that executes the operation
   * @param policy      retry policy configuration
   * @param nextAttempt the next attempt number to execute
   * @param backoff     delay before retrying
   * @param <T>         result type
   * @return completion stage with the delayed retry result
   */
  private static <T> CompletionStage<T> delayedRetry(Supplier<CompletionStage<T>> operation, RetryPolicy policy, int nextAttempt, Duration backoff) {
    return CompletableFuture.supplyAsync(() -> null, delayedExecutor(backoff.toMillis(), TimeUnit.MILLISECONDS))
                            .thenCompose(ignored -> executeAttempt(operation, policy, nextAttempt));
  }

  /**
   * Determines whether a retry should be attempted based on the error type and attempt count.
   *
   * @param cause   the error that occurred
   * @param attempt current attempt number
   * @param policy  retry policy configuration
   * @return true if retry should be attempted, false otherwise
   */
  private static boolean shouldRetry(Throwable cause, int attempt, RetryPolicy policy) {
    if (attempt >= policy.maxAttempts()) {
      log.warn("Max retry attempts exhausted after {} attempts", policy.maxAttempts(), cause);
      return false;
    }

    if (!isRetryable(cause)) {
      log.debug("Error is not retryable:{}. Failing immediately after attempt {}", cause.getClass().getSimpleName(), attempt, cause);
      return false;
    }

    return true;
  }

  /**
   * Calculates the exponential backoff delay for the given attempt number.
   * <p>
   * The backoff is calculated as: initialBackoff × (multiplier ^ (attempt - 1))
   * and capped at maxBackoff.
   *
   * @param policy  retry policy with backoff configuration
   * @param attempt current attempt number (1-based)
   * @return the calculated backoff duration, capped at maxBackoff
   */
  private static Duration calculateBackoff(RetryPolicy policy, int attempt) {
    if (attempt <= 1) return policy.initialBackoff();

    var multiplier = Math.pow(policy.backoffMultiplier(), attempt - 1);
    var backoffMillis = (long) (policy.initialBackoff().toMillis() * multiplier);
    var cappedMillis = Math.min(backoffMillis, policy.maxBackoff().toMillis());

    return Duration.ofMillis(cappedMillis);
  }

  /**
   * Unwraps nested CompletionException and ArmoniKException to find the root cause.
   * <p>
   * This is necessary because CompletionStage operations often wrap exceptions,
   * and we need the actual gRPC StatusRuntimeException to determine retryability.
   *
   * @param throwable the exception to unwrap
   * @return the root cause exception
   */
  private static Throwable unwrapException(Throwable throwable) {
    Throwable current = throwable;
    while (current.getCause() != null) {
      if (current instanceof CompletionException || current instanceof ArmoniKException) {
        current = current.getCause();
      } else {
        break;
      }
    }

    return current;
  }
}
