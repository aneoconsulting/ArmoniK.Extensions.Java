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

import fr.aneo.armonik.client.BlobCompletionListener;
import fr.aneo.armonik.client.definition.TaskDefinition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * The download budget bounds the data of downloaded outputs: 80 outputs of 4 MB (320 MB) complete
 * faster than a listener that takes 1 s per output, with a 64 MB budget.
 * <p>
 * The failsafe execution {@code memory} runs this class alone in a JVM whose heap is capped at
 * 256 MB, where downloading every output at once runs out of memory. Expected: every output reaches
 * the listener as a success, none as an error. The peak of the heap in use is recorded, not asserted:
 * it includes garbage not yet collected, so it varies from one run to the next.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 10, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class MemoryIT {
  private static final int OUTPUTS = 80;
  private static final int SIZE = 4_000_000;
  private static final long BUDGET = 64L * 1024 * 1024;

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  void outputs_completing_faster_than_the_listener_are_all_downloaded_within_the_budget() {
    var maxHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
    metric("MemoryIT", "max_heap_MB", maxHeapMb);
    try (var client = newClient(builder -> builder.withDownloadBudget(BUDGET))) {
      var successes = new AtomicInteger();
      List<String> errors = new CopyOnWriteArrayList<>();
      var listener = new BlobCompletionListener() {
        @Override
        public void onSuccess(Blob blob) {
          sleep(1_000);
          successes.incrementAndGet();
        }

        @Override
        public void onError(BlobError blobError) {
          errors.add(String.valueOf(blobError.cause()));
        }
      };
      var session = openSession(client, listener);
      var definitions = new ArrayList<TaskDefinition>();
      for (int i = 0; i < OUTPUTS; i++) definitions.add(task(session, utf8("m" + i), "big:" + SIZE));

      var memory = ManagementFactory.getMemoryMXBean();
      var peak = new AtomicLong();
      var sampling = new AtomicBoolean(true);
      var sampler = new Thread(() -> {
        while (sampling.get()) {
          peak.accumulateAndGet(memory.getHeapMemoryUsage().getUsed(), Math::max);
          sleep(20);
        }
      }, "it-heap-sampler");
      sampler.setDaemon(true);
      sampler.start();

      var start = System.nanoTime();
      session.submitTasks(definitions);
      try {
        run(Duration.ofMinutes(8), "awaitOutputsProcessed of 80 x 4 MB outputs with a slow listener", session::awaitOutputsProcessed);
      } finally {
        sampling.set(false);
        metric("MemoryIT", "outputs_80x4MB_slow_listener_ms", millisSince(start));
        metric("MemoryIT", "heap_peak_used_MB", peak.get() / (1024 * 1024));
        metric("MemoryIT", "successes", successes.get());
        metric("MemoryIT", "errors", errors.size());
      }
      within(Duration.ofSeconds(30), "cancel", session.cancel());
      within(Duration.ofSeconds(30), "purge", session.purge());
      assertThat(errors).as("outputs reported as failed, with a %d MB heap", maxHeapMb).isEmpty();
      assertThat(successes.get()).isEqualTo(OUTPUTS);
    }
  }
}
