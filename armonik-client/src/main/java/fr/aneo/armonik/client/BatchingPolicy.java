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

import fr.aneo.armonik.client.definition.SessionDefinition;

import java.time.Duration;

/**
 * Immutable policy that defines when buffered items should be flushed and how they are grouped into batches.
 * <p>
 * This policy is used by {@link BlobCompletionCoordinator} to optimize blob completion monitoring operations
 * through efficient batching strategies. It provides fine-grained control over the trade-offs between
 * latency, throughput, and resource utilization.
 *
 * <h2>Policy Parameters</h2>
 * <ul>
 *   <li><strong>Size trigger</strong> — {@link #batchSize()}: when the number of buffered items
 *       reaches this threshold, a flush is triggered. This is a <em>threshold</em>, not a cap.</li>
 *
 *   <li><strong>Time trigger</strong> — {@link #maxDelay()}: even if the size trigger is not
 *       reached, a flush occurs once the oldest item has waited this long.</li>
 *
 *   <li><strong>Parallelism</strong> — {@link #maxConcurrentBatches()}: at most this many batch
 *       flush operations may be in flight at the same time.</li>
 *
 *   <li><strong>Per-batch cap</strong> — {@link #capPerBatch()}: an upper bound on how many items
 *       are included in a <em>single</em> batch flush. If more items are available, they are split
 *       into multiple batches, each no larger than this value.</li>
 * </ul>
 *
 * <h2>Batching Semantics</h2>
 * <ul>
 *   <li>A flush is initiated when <em>either</em> the size trigger is reached
 *       (<code>bufferedItems &gt;= batchSize</code>) <em>or</em> the time trigger expires
 *       (<code>oldestItemAge &gt;= maxDelay</code>).</li>
 *   <li>On flush, buffered items may be divided into multiple chunks according to
 *       {@link #capPerBatch()}. Each chunk is processed as a separate batch, subject to
 *       {@link #maxConcurrentBatches()}.</li>
 *   <li>Use {@link #batchSize()} and {@link #maxDelay()} to trade <em>latency</em> for
 *       <em>throughput</em>: smaller values flush sooner; larger values amortize overhead better.</li>
 *   <li>Use {@link #capPerBatch()} to keep individual batches within safe/efficient limits
 *       (e.g., gRPC message size limits or memory constraints).</li>
 *   <li>Use {@link #maxConcurrentBatches()} to control overall concurrency independently of size
 *       and time triggers.</li>
 * </ul>
 *
 * <h2>Usage Context</h2>
 * <p>
 * BatchingPolicy is primarily used within {@link SessionDefinition} to configure how task output
 * completion events are batched for efficient processing. The policy affects the responsiveness
 * and resource usage of blob completion monitoring operations.
 *
 * <p><strong>Thread-safety:</strong> This class is immutable and therefore thread-safe.
 *
 * @param batchSize             size-based flush trigger (threshold; must be &gt; 0)
 * @param maxDelay              time-based flush trigger (max buffering delay; must be &gt; 0)
 * @param maxConcurrentBatches  maximum number of batches processed concurrently (must be &gt; 0)
 * @param capPerBatch           hard cap on the number of items included in a single batch (must be &gt; 0)
 * @see BlobCompletionCoordinator
 * @see SessionDefinition
 */
record BatchingPolicy(
  int batchSize,
  Duration maxDelay,
  int maxConcurrentBatches,
  int capPerBatch
) {
  /**
   * A conservative default policy that balances responsiveness with throughput efficiency.
   * <p>
   * The default values are optimized for typical blob completion monitoring scenarios:
   * <ul>
   *   <li><strong>batchSize = 50</strong> — triggers flush after 50 items, providing good throughput</li>
   *   <li><strong>maxDelay = 200ms</strong> — ensures responsive processing even with low item rates</li>
   *   <li><strong>maxConcurrentBatches = 5</strong> — allows moderate parallelism without overwhelming the system</li>
   *   <li><strong>capPerBatch = 100</strong> — keeps individual batch sizes manageable for gRPC operations</li>
   * </ul>
   * These defaults work well for most applications but can be customized based on specific
   * performance requirements and cluster characteristics.
   *
   * @see SessionDefinition
   */
   static final BatchingPolicy DEFAULT = new BatchingPolicy(50, Duration.ofMillis(200), 5, 100);

  /**
   * Validates all policy parameters and their relationships.
   * <p>
   * This compact constructor ensures that:
   * <ul>
   *   <li>All numeric values are positive</li>
   *   <li>The delay duration is positive and non-null</li>
   *   <li>Parameter values are within reasonable bounds</li>
   * </ul>
   *
   * @throws IllegalArgumentException if {@code batchSize}, {@code maxConcurrentBatches},
   *                                  or {@code capPerBatch} is {@code <= 0}
   * @throws IllegalArgumentException if {@code maxDelay} is {@code null}, zero, or negative
   */
   BatchingPolicy {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be > 0, got: " + batchSize);
    }
    if (maxConcurrentBatches <= 0) {
      throw new IllegalArgumentException("maxConcurrentBatches must be > 0, got: " + maxConcurrentBatches);
    }
    if (capPerBatch <= 0) {
      throw new IllegalArgumentException("capPerBatch must be > 0, got: " + capPerBatch);
    }
    if (maxDelay == null || maxDelay.isZero() || maxDelay.isNegative()) {
      throw new IllegalArgumentException("maxDelay must be > 0, got: " + maxDelay);
    }
  }
}
