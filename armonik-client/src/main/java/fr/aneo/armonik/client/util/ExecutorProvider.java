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

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import static java.lang.Runtime.getRuntime;

/**
 * Provides a shared {@link Executor} optimized for the ArmoniK Java client.
 * <p>
 * The default executor is a daemon {@link ForkJoinPool} whose size scales with available
 * processors (with a minimum parallelism of 2). Threads are named using the
 * {@code "ArmoniK-Client-<index>"} pattern to ease troubleshooting.
 * </p>
 *
 * <p>
 * This executor is intended to be used for lightweight asynchronous work.
 * </p>
 */
public final class ExecutorProvider {
  private static final Executor EXECUTOR = createDefault();

  private ExecutorProvider() {
  }


  /**
   * Returns the shared, daemon {@link Executor} used by the client for callbacks and
   * lightweight asynchronous work.
   *
   * @return the default shared executor
   */
  public static Executor defaultExecutor() {
    return EXECUTOR;
  }

  private static Executor createDefault() {
    int cores = Math.max(2, getRuntime().availableProcessors());
    var factory = (ForkJoinPool.ForkJoinWorkerThreadFactory) pool -> {
      var workerThread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
      workerThread.setName("ArmoniK-Client" + "-" + workerThread.getPoolIndex());
      workerThread.setDaemon(true);
      return workerThread;
    };
    return new ForkJoinPool(cores, factory, null, true);
  }
}

