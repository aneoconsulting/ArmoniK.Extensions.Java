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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * Mail scenario 4: "hang once leaked streams fill SETTINGS_MAX_CONCURRENT_STREAMS", and the long run
 * without stall: 20 sessions of one client loop on submit one task + awaitOutputsProcessed.
 * <p>
 * Expected: every round finishes; no 60 s without progress. Kestrel allows 100 streams per
 * connection, so with leaked streams the mail predicts a stall after 5,000 to 6,400 rounds. The
 * stress run does 6,000 rounds, the default run 600.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 40, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class MailStreamLimitIT {
  private static final Duration NO_PROGRESS_LIMIT = Duration.ofSeconds(60);

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  void twenty_sessions_doing_thousands_of_submit_and_wait_rounds_never_stall() throws Exception {
    var target = STRESS ? 6_000 : 600;
    var workers = 20;
    var completed = new AtomicInteger();
    var lastProgress = new AtomicLong(System.nanoTime());
    var failure = new AtomicReference<Throwable>();
    var peakEventChannels = new AtomicInteger();
    var peakConnections = new AtomicInteger();
    try (var client = newClient()) {
      var threads = new ArrayList<Thread>();
      for (int worker = 0; worker < workers; worker++) {
        var index = worker;
        var thread = new Thread(() -> {
          try {
            var session = openSession(client, null);
            var round = 0;
            while (completed.get() < target && failure.get() == null) {
              session.submitTasks(copyTasks(session, 1, "s" + index + "r" + round++ + "-"));
              session.awaitOutputsProcessed();
              completed.incrementAndGet();
              lastProgress.set(System.nanoTime());
            }
          } catch (Throwable throwable) {
            failure.compareAndSet(null, throwable);
          }
        }, "it-rounds-" + worker);
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();
      }

      var start = System.nanoTime();
      var lastReport = 0L;
      while (completed.get() < target && failure.get() == null
        && System.nanoTime() - lastProgress.get() < NO_PROGRESS_LIMIT.toNanos()) {
        Thread.sleep(200);
        peakEventChannels.accumulateAndGet(channelsInUse(client, "eventChannelPool"), Math::max);
        peakConnections.accumulateAndGet(connectionsTo(endpointPort()), Math::max);
        if (millisSince(start) - lastReport > 10_000) {
          lastReport = millisSince(start);
          System.out.printf("  %d/%d rounds after %d s%n", completed.get(), target, lastReport / 1000);
        }
      }
      var elapsed = millisSince(start);
      metric("MailStreamLimitIT", "rounds_completed", completed.get() + "/" + target);
      metric("MailStreamLimitIT", "rounds_per_s", String.format("%.1f", completed.get() * 1000.0 / Math.max(1, elapsed)));
      metric("MailStreamLimitIT", "peak_event_channels_in_use", peakEventChannels.get());
      metric("MailStreamLimitIT", "peak_tcp_connections", peakConnections.get());

      assertThat(failure.get()).as("a round failed").isNull();
      assertThat(completed.get()).as("rounds done before %d s without progress. %s", NO_PROGRESS_LIMIT.toSeconds(), sdkThreads())
                                 .isGreaterThanOrEqualTo(target);
      for (var thread : threads) thread.join(10_000);
    }
  }
}
