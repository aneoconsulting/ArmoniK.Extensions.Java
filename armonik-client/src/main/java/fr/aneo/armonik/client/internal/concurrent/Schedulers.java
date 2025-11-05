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

import fr.aneo.armonik.client.BatchingPolicy;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Utility class providing shared scheduled executor services for the ArmoniK client.
 * <p>
 * This class manages a singleton {@link ScheduledThreadPoolExecutor} instance that is used
 * throughout the ArmoniK client for scheduling delayed operations, timers, and periodic tasks.
 * The shared executor is optimized for the client's batching and coordination needs.
 * <p>
 * The shared scheduler is configured with:
 * <ul>
 *   <li><strong>Single thread:</strong> Uses one daemon thread to minimize resource overhead</li>
 *   <li><strong>Daemon thread:</strong> Won't prevent JVM shutdown</li>
 *   <li><strong>Optimized cleanup:</strong> Removes canceled tasks immediately to prevent memory leaks</li>
 *   <li><strong>Shutdown behavior:</strong> Doesn't execute delayed tasks after shutdown</li>
 * </ul>
 * <p>
 * A JVM shutdown hook is automatically registered to properly shut down the executor when
 * the application terminates, ensuring clean resource cleanup.
 *
 * @see BatchingPolicy
 */
public class Schedulers {

  /**
   * The shared scheduled thread pool executor instance.
   * <p>
   * This executor is configured as a single-threaded daemon scheduler with optimized
   * cleanup policies for the ArmoniK client's scheduling needs.
   */
  private static final ScheduledThreadPoolExecutor SHARED;

  private Schedulers() {}

  static {
    SHARED = new ScheduledThreadPoolExecutor(
      1,
      r -> {
        Thread t = new Thread(r, "armonik-scheduler");
        t.setDaemon(true);
        return t;
      }
    );
    SHARED.setRemoveOnCancelPolicy(true);
    SHARED.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

    Runtime.getRuntime().addShutdownHook(new Thread(SHARED::shutdown, "armonik-scheduler-shutdown"));
  }

  /**
   * Returns the shared scheduled executor service for the ArmoniK client.
   * <p>
   * This method provides access to the singleton {@link ScheduledExecutorService} instance
   * that is used throughout the client for scheduling operations. The executor is:
   * <ul>
   *   <li><strong>Thread-safe:</strong> Can be safely accessed from multiple threads</li>
   *   <li><strong>Singleton:</strong> The same instance is returned on every call</li>
   *   <li><strong>Lifecycle-managed:</strong> Automatically shutdown during JVM termination</li>
   * </ul>
   * <p>
   * <strong>Usage Guidelines:</strong>
   * <ul>
   *   <li>Do not call {@code shutdown()} on the returned executor</li>
   *   <li>Prefer this over creating new schedulers for better resource utilization</li>
   *   <li>Use {@code schedule()} methods for delayed operations</li>
   *   <li>Use {@code scheduleWithFixedDelay()} for periodic operations with cleanup needs</li>
   * </ul>
   *
   * @return the shared scheduled executor service
   */
  public static ScheduledExecutorService shared() {
    return SHARED;
  }
}
