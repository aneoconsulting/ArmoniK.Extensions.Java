/*
 * Copyright (C) 2025 ANEO (armonik@aneo.fr)
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
package fr.aneo.armonik.client.it;

import fr.aneo.armonik.client.ArmoniKClient;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * Mail scenario 1: "throughput limited by a 50-channel pool lending one channel per call".
 * <p>
 * Expected: submitting thousands of tasks never parks a thread waiting for a pooled channel, and the
 * submission is not serialized behind the pool. The sessions are paused, so the numbers measure the
 * submission path only (client and control plane), not the worker.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 10, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class MailThroughputIT {

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  /** The mail's probe: 2,000 tasks with one small input each in one submitTasks call. */
  @Test
  void submitting_2000_small_tasks_never_blocks_a_thread_on_the_channel_pool() {
    try (var client = newClient()) {
      var session = openSession(client, null);
      within(Duration.ofSeconds(30), "pause", session.pause());
      var definitions = copyTasks(session, 2_000, "probe-");
      var sampler = new PoolSampler(client).start();
      var start = System.nanoTime();
      var handles = call(Duration.ofMinutes(1), "submitTasks", () -> session.submitTasks(definitions));
      within(Duration.ofMinutes(5), "acknowledgement of 2,000 tasks", allTaskInfos(handles));
      var elapsed = millisSince(start);
      sampler.stop();

      metric("MailThroughputIT", "small_2000_ack_ms", elapsed);
      metric("MailThroughputIT", "small_2000_tasks_per_s", 2_000_000 / Math.max(1, elapsed));
      metric("MailThroughputIT", "small_2000_peak_threads_blocked_in_pool", sampler.peakBlocked.get());
      metric("MailThroughputIT", "small_2000_peak_channels_in_use", sampler.peakInUse.get());
      metric("MailThroughputIT", "small_2000_peak_tcp_connections", sampler.peakConnections.get());
      assertThat(sampler.peakBlocked.get()).as("threads parked in ManagedChannelPool while submitting").isZero();
    }
  }

  /**
   * Inputs above the inline threshold (the control plane's DataChunkMaxSize, 2 MiB by default) are
   * still uploaded one stream each, one stream per pooled channel (one connection), at most
   * channelPoolSize at a time. Compares 50 with 200 channels: a large gain would mean the per-call
   * channel lending caps throughput. The sessions are purged afterwards to free the object storage.
   */
  @Test
  void streamed_uploads_are_not_capped_by_the_channel_pool() {
    var tasks = 80;
    var sizes = new long[2];
    var poolSizes = new int[]{50, 200};
    for (int run = 0; run < poolSizes.length; run++) {
      var poolSize = poolSizes[run];
      try (var client = newClient(builder -> builder.withChannelPoolSize(poolSize))) {
        var session = openSession(client, null);
        within(Duration.ofSeconds(30), "pause", session.pause());
        var library = library(session);
        var random = new Random(run);
        var definitions = new ArrayList<TaskDefinition>();
        for (int i = 0; i < tasks; i++) {
          var data = new byte[2_200_000];
          random.nextBytes(data);
          definitions.add(new TaskDefinition().withInput("A", InputBlobDefinition.from(data)).withOutput("result").withWorkerLibrary(library));
        }
        var sampler = new PoolSampler(client).start();
        var start = System.nanoTime();
        var handles = session.submitTasks(definitions);
        within(Duration.ofMinutes(5), tasks + " tasks with 2.2 MB inputs", allTaskInfos(handles));
        sizes[run] = millisSince(start);
        sampler.stop();
        within(Duration.ofSeconds(60), "cancel", session.cancel());
        within(Duration.ofSeconds(60), "purge", session.purge());
        metric("MailThroughputIT", "streamed_80x2200KB_pool" + poolSize + "_ms", sizes[run]);
        metric("MailThroughputIT", "streamed_80x2200KB_pool" + poolSize + "_peak_channels_in_use", sampler.peakInUse.get());
        metric("MailThroughputIT", "streamed_80x2200KB_pool" + poolSize + "_peak_tcp_connections", sampler.peakConnections.get());
        assertThat(sampler.peakBlocked.get()).as("threads parked in ManagedChannelPool").isZero();
      }
    }
    // Recorded rather than asserted: the cluster is shared and timings are noisy
    metric("MailThroughputIT", "streamed_speedup_pool200_vs_pool50", String.format("%.2f", (double) sizes[0] / Math.max(1, sizes[1])));
  }

  /** Samples the SDK's pool every 20 ms. */
  static final class PoolSampler {
    final ArmoniKClient client;
    final AtomicInteger peakBlocked = new AtomicInteger();
    final AtomicInteger peakInUse = new AtomicInteger();
    final AtomicInteger peakConnections = new AtomicInteger();
    final AtomicBoolean running = new AtomicBoolean(true);
    Thread thread;

    PoolSampler(ArmoniKClient client) {
      this.client = client;
    }

    PoolSampler start() {
      thread = new Thread(() -> {
        while (running.get()) {
          peakBlocked.accumulateAndGet(threadsBlockedInPool(), Math::max);
          peakInUse.accumulateAndGet(channelsInUse(client, "channelPool"), Math::max);
          peakConnections.accumulateAndGet(connectionsTo(endpointPort()), Math::max);
          sleep(20);
        }
      }, "it-pool-sampler");
      thread.setDaemon(true);
      thread.start();
      return this;
    }

    void stop() {
      running.set(false);
      try {
        thread.join(1_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

}
