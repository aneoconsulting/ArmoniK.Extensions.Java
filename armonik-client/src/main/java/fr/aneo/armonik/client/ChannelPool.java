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

import io.grpc.ManagedChannel;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Pool of gRPC channels for managing concurrent connections to the ArmoniK cluster.
 * <p>
 * This interface provides channel pooling capabilities to optimize resource utilization
 * and improve performance when making multiple concurrent requests to the ArmoniK cluster.
 * Instead of creating a new channel for each operation, channels are reused from a pool,
 * reducing connection overhead and improving throughput.
 * <p>
 * <strong>Design Philosophy:</strong> This pool uses a <strong>functional API</strong> pattern
 * where operations are passed as functions/lambdas. This ensures channels are always properly
 * returned to the pool, preventing resource leaks even when exceptions occur.
 * Implementations must ensure thread-safety and proper lifecycle management of pooled channels.
 *
 * @see ManagedChannel
 * @see ManagedChannelPool
 */
public interface ChannelPool extends AutoCloseable {

  /**
   * Executes an operation using a channel from the pool.
   * <p>
   * This is the <strong>primary and recommended way</strong> to use the channel pool. The method
   * automatically manages the channel lifecycle: acquires a channel, executes the provided operation,
   * and ensures the channel is properly returned to the pool regardless of whether the operation
   * succeeds or fails. This pattern prevents channel leaks and simplifies resource management.
   * <p>
   * The operation is provided as a function that receives the channel and returns a result.
   * The channel is guaranteed to be healthy and ready to use. If an exception occurs during
   * the operation, the channel is still properly returned to the pool before re-throwing
   * the exception.
   * <p>
   * <strong>Example usage:</strong>
   * <pre>{@code
   * String sessionId = channelPool.execute(channel -> {
   *   var stub = SessionsGrpc.newBlockingStub(channel);
   *   var response = stub.createSession(request);
   *   return response.getSessionId();
   * });
   * }</pre>
   *
   * @param operation the operation to execute with the acquired channel
   * @param <T>       the type of result produced by the operation
   * @return the result of the operation
   * @throws NullPointerException if operation is null
   * @throws RuntimeException if the operation fails or channel acquisition fails
   */
  <T> T execute(Function<ManagedChannel, T> operation);

  /**
   * Executes an asynchronous operation using a channel from the pool.
   * <p>
   * Similar to {@link #execute(Function)}, but for asynchronous operations that return
   * a {@link CompletionStage}. This is the <strong>recommended way for async operations</strong>.
   * The channel is automatically acquired before the operation starts and released when the
   * completion stage completes (either successfully or exceptionally).
   * <p>
   * This method is ideal for non-blocking gRPC calls using future stubs. The channel
   * lifecycle is tied to the completion stage lifecycle, ensuring proper cleanup even
   * when async operations fail.
   * <p>
   * <strong>Example usage:</strong>
   * <pre>{@code
   * CompletionStage<String> sessionId = channelPool.executeAsync(channel -> {
   *   var stub = SessionsGrpc.newFutureStub(channel);
   *   return Futures.toCompletionStage(stub.createSession(request))
   *                 .thenApply(response -> response.getSessionId());
   * });
   * }</pre>
   *
   * @param operation the asynchronous operation to execute with the acquired channel
   * @param <T>       the type of result produced by the operation
   * @return a completion stage that completes with the operation result
   * @throws NullPointerException if operation is null
   */
  <T> CompletionStage<T> executeAsync(Function<ManagedChannel, CompletionStage<T>> operation);

  /**
   * Shuts down the channel pool and releases all resources.
   * <p>
   * This method initiates an orderly shutdown of all channels in the pool. Any operations
   * currently in progress are allowed to complete, but new operations will fail. The method
   * attempts to wait for graceful shutdown within a timeout period. If interrupted during
   * shutdown, the method preserves the interrupt status and forces immediate shutdown.
   * <p>
   * After calling this method, the pool cannot be reused and all subsequent operations
   * will throw {@link IllegalStateException}.
   */
  @Override
  void close();

  /**
   * Returns the current number of channels in the pool (both idle and in-use).
   * <p>
   * The returned value is a snapshot and may change immediately after this method
   * returns due to concurrent operations.
   *
   * @return the total number of channels managed by this pool
   */
  int size();

  /**
   * Returns the number of idle channels currently available in the pool.
   * <p>
   * Idle channels are ready to be acquired without creating new connections.
   * This metric is useful for monitoring pool utilization and tuning pool sizing.
   *
   * @return the number of channels available for immediate acquisition
   */
  int availableChannels();
}
