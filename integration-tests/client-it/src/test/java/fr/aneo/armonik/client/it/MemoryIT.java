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

import fr.aneo.armonik.client.ArmoniKConfig;
import fr.aneo.armonik.client.BlobCompletionListener;
import fr.aneo.armonik.client.definition.TaskDefinition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * Data held by the client when outputs complete faster than the listener takes them: 80 outputs of
 * 4 MB (320 MB), a listener taking 1 s each. Expected: the client holds a bounded amount of output data.
 * PR #71 adds ArmoniKConfig.withDownloadBudget, set here to 64 MB through reflection where it exists.
 * Stress only.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#stressEnabled")
@Timeout(value = 10, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class MemoryIT {
  private static final int OUTPUTS = 80;
  private static final int SIZE = 4_000_000;

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  void outputs_completing_faster_than_the_listener_keep_the_heap_bounded() {
    var budget = new AtomicBoolean();
    try (var client = newClient(builder -> budget.set(setBudget(builder, 64L * 1024 * 1024)))) {
      var delivered = new AtomicInteger();
      var listener = new BlobCompletionListener() {
        @Override
        public void onSuccess(Blob blob) {
          sleep(1_000);
          delivered.incrementAndGet();
        }

        @Override
        public void onError(BlobError blobError) {
          delivered.incrementAndGet();
        }
      };
      var session = openSession(client, listener);
      var definitions = new ArrayList<TaskDefinition>();
      for (int i = 0; i < OUTPUTS; i++) definitions.add(task(session, utf8("m" + i), "big:" + SIZE));

      System.gc();
      var memory = ManagementFactory.getMemoryMXBean();
      var base = memory.getHeapMemoryUsage().getUsed();
      var peak = new AtomicLong();
      var sampling = new AtomicBoolean(true);
      var sampler = new Thread(() -> {
        while (sampling.get()) {
          peak.accumulateAndGet(memory.getHeapMemoryUsage().getUsed() - base, Math::max);
          sleep(20);
        }
      }, "it-heap-sampler");
      sampler.setDaemon(true);
      sampler.start();

      var start = System.nanoTime();
      session.submitTasks(definitions);
      run(Duration.ofMinutes(8), "awaitOutputsProcessed of 80 x 4 MB outputs with a slow listener", session::awaitOutputsProcessed);
      sampling.set(false);
      var peakMb = peak.get() / (1024 * 1024);
      metric("MemoryIT", "budget_api_available", budget.get());
      metric("MemoryIT", "outputs_80x4MB_slow_listener_ms", millisSince(start));
      metric("MemoryIT", "heap_peak_above_base_MB", peakMb);
      within(Duration.ofSeconds(30), "cancel", session.cancel());
      within(Duration.ofSeconds(30), "purge", session.purge());
      assertThat(delivered.get()).isEqualTo(OUTPUTS);
      assertThat(peakMb).as("peak heap above baseline while 320 MB of outputs completed").isLessThan(160);
    }
  }

  private static boolean setBudget(ArmoniKConfig.Builder builder, long bytes) {
    try {
      ArmoniKConfig.Builder.class.getMethod("withDownloadBudget", long.class).invoke(builder, bytes);
      return true;
    } catch (ReflectiveOperationException e) {
      return false;
    }
  }
}
