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
import java.util.concurrent.TimeUnit;

import static fr.aneo.armonik.client.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * Mail scenario 2: "GetEvents streams are never cancelled".
 * <p>
 * Expected: once awaitOutputsProcessed returns, no event stream holds a channel of the event pool
 * (a subscription holds its channel exactly as long as its GetEvents call is open), and closing the
 * client takes about as long as closing a client that did nothing.
 */
@EnabledIf("fr.aneo.armonik.client.it.ItSupport#enabled")
@Timeout(value = 5, unit = TimeUnit.MINUTES, threadMode = SEPARATE_THREAD)
class MailLeakIT {

  @AfterAll
  static void cleanUp() {
    cancelCreatedSessions();
  }

  @Test
  void event_streams_end_once_outputs_are_processed_and_close_is_fast() {
    var baseline = newClient();
    openSession(baseline, null);
    var baselineStart = System.nanoTime();
    baseline.close();
    var baselineCloseMillis = millisSince(baselineStart);

    var client = newClient();
    var session = openSession(client, null);
    var rounds = 20;
    var roundsStart = System.nanoTime();
    for (int round = 0; round < rounds; round++) {
      session.submitTasks(copyTasks(session, 1, "leak-" + round + "-"));
      run(Duration.ofMinutes(1), "round " + round, session::awaitOutputsProcessed);
    }
    metric("MailLeakIT", "round_avg_ms", millisSince(roundsStart) / rounds);

    var released = await(Duration.ofSeconds(2), () -> channelsInUse(client, "eventChannelPool") == 0);
    var heldEventChannels = channelsInUse(client, "eventChannelPool");
    var connectionsBefore = connectionsTo(endpointPort());
    var closeStart = System.nanoTime();
    client.close();
    var closeMillis = millisSince(closeStart);
    await(Duration.ofSeconds(2), () -> connectionsTo(endpointPort()) == 0);

    metric("MailLeakIT", "event_channels_held_after_rounds", heldEventChannels);
    metric("MailLeakIT", "connections_before_close", connectionsBefore);
    metric("MailLeakIT", "close_ms", closeMillis);
    metric("MailLeakIT", "baseline_close_ms", baselineCloseMillis);
    metric("MailLeakIT", "connections_after_close", connectionsTo(endpointPort()));

    assertThat(released).as("event channels still held by GetEvents streams after every output was processed: %d", heldEventChannels).isTrue();
    assertThat(closeMillis).as("close() after 20 rounds, baseline %d ms", baselineCloseMillis).isLessThan(baselineCloseMillis + 1_000);
    assertThat(connectionsTo(endpointPort())).as("TCP connections left open after close()").isZero();
  }
}
