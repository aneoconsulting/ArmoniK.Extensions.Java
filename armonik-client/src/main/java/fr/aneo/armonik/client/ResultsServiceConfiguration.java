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

import fr.aneo.armonik.api.grpc.v1.Objects.Empty;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Reads the Results service configuration of the control plane once and shares it.
 * <p>
 * Its {@code DataChunkMaxSize} bounds the requests that carry blob data inline: a {@code CreateResults}
 * request no larger than this size replaces a metadata call followed by an upload stream per blob.
 * When the configuration cannot be read, the size is reported as 0, which disables inline creation,
 * and the next caller tries again.
 */
final class ResultsServiceConfiguration {
  private static final Logger logger = LoggerFactory.getLogger(ResultsServiceConfiguration.class);

  private final ChannelPool channelPool;
  private final AtomicReference<CompletableFuture<Integer>> dataChunkMaxSize = new AtomicReference<>();

  ResultsServiceConfiguration(ChannelPool channelPool) {
    this.channelPool = requireNonNull(channelPool, "channelPool must not be null");
  }

  /**
   * Returns the maximum size, in bytes, of a request carrying blob data, as configured on the control plane.
   *
   * @return a stage completing with that size, or with 0 when the configuration could not be read; it never fails
   */
  CompletionStage<Integer> dataChunkMaxSize() {
    var current = dataChunkMaxSize.get();
    if (current != null) return current;

    var created = new CompletableFuture<Integer>();
    if (!dataChunkMaxSize.compareAndSet(null, created)) return dataChunkMaxSize.get();

    try {
      channelPool.executeAsync(channel -> Futures.toCompletionStage(ResultsGrpc.newFutureStub(channel).getServiceConfiguration(Empty.getDefaultInstance())))
                 .whenComplete((response, error) -> {
                   if (error == null) {
                     logger.debug("Results service configuration read: DataChunkMaxSize={}", response.getDataChunkMaxSize());
                     created.complete(response.getDataChunkMaxSize());
                   } else {
                     fallBack(created, error);
                   }
                 });
    } catch (RuntimeException e) {
      fallBack(created, e);
    }
    return created;
  }

  private void fallBack(CompletableFuture<Integer> created, Throwable error) {
    logger.warn("Could not read the Results service configuration, blobs are created without inline data", error);
    dataChunkMaxSize.compareAndSet(created, null);
    created.complete(0);
  }
}
