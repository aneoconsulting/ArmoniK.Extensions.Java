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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DownloadBudgetTest {

  @Test
  @DisplayName("should admit at once what fits, and hold the rest until bytes are released")
  void should_admit_what_fits_and_hold_the_rest() {
    var budget = new DownloadBudget(100);

    var first = budget.acquire("a", 60);
    var second = budget.acquire("a", 60);

    assertThat(first).isCompleted();
    assertThat(second).isNotDone();
    assertThat(budget.used()).isEqualTo(60);

    budget.release(60);

    assertThat(second).isCompleted();
    assertThat(budget.used()).isEqualTo(60);
  }

  @Test
  @DisplayName("should admit a reservation larger than the budget alone, once nothing is reserved")
  void should_admit_oversized_reservation_alone() {
    var budget = new DownloadBudget(100);
    var small = budget.acquire("a", 10);

    var large = budget.acquire("b", 500);
    var afterLarge = budget.acquire("c", 10);

    assertThat(small).isCompleted();
    assertThat(large).isNotDone();
    // Later reservations do not overtake the large one
    assertThat(afterLarge).isNotDone();

    budget.release(10);
    assertThat(large).isCompleted();
    assertThat(afterLarge).isNotDone();

    budget.release(500);
    assertThat(afterLarge).isCompleted();
    assertThat(budget.used()).isEqualTo(10);
  }

  @Test
  @DisplayName("should admit an empty output while an oversized one holds the budget")
  void should_admit_empty_output_beside_oversized_one() {
    var budget = new DownloadBudget(100);
    budget.acquire("a", 500);

    assertThat(budget.acquire("b", 0)).isCompleted();
  }

  @Test
  @DisplayName("should fail only the waiting reservations of a cancelled session, and admit those they held back")
  void should_fail_waiting_reservations_of_cancelled_session() {
    // Given: a reservation of session C waits for the budget, and holds back B's
    var budget = new DownloadBudget(100);
    var admittedOfA = budget.acquire("a", 80);
    var waitingOfC = budget.acquire("c", 80);
    var waitingOfB = budget.acquire("b", 20);

    assertThat(waitingOfB).isNotDone();

    // When
    budget.cancel("c", new IllegalStateException("closed"));

    // Then
    assertThat(admittedOfA).isCompleted();
    assertThat(waitingOfC).isCompletedExceptionally();
    assertThat(waitingOfB).isCompleted();
    assertThat(budget.waiting()).isZero();
  }

  @Test
  @DisplayName("should admit everything at once when unlimited")
  void should_admit_everything_when_unlimited() {
    var budget = DownloadBudget.unlimited();

    CompletableFuture<Void> huge = budget.acquire("a", Long.MAX_VALUE);

    assertThat(budget.isUnlimited()).isTrue();
    assertThat(huge).isCompleted();
    assertThat(new DownloadBudget(1).isUnlimited()).isFalse();
  }

  @Test
  @DisplayName("should reject a capacity below 1")
  void should_reject_capacity_below_one() {
    assertThatThrownBy(() -> new DownloadBudget(0)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should not keep the bytes of a reservation cancelled while it waits")
  void should_not_keep_bytes_of_cancelled_reservation() {
    var budget = new DownloadBudget(10);
    budget.acquire("a", 10);
    var waiting = budget.acquire("a", 5);

    waiting.cancel(false);
    budget.release(10);

    assertThat(budget.used()).isZero();
    assertThat(budget.waiting()).isZero();
  }

  @Test
  @DisplayName("should not keep the bytes of a reservation timed out while it waits")
  void should_not_keep_bytes_of_timed_out_reservation() {
    var budget = new DownloadBudget(10);
    budget.acquire("a", 10);
    var waiting = budget.acquire("a", 5).orTimeout(1, TimeUnit.MILLISECONDS);

    assertThatThrownBy(waiting::join).hasCauseInstanceOf(TimeoutException.class);
    budget.release(10);

    assertThat(budget.used()).isZero();
    assertThat(budget.waiting()).isZero();
  }

  @Test
  @DisplayName("should let later reservations in at once when the one holding them back is cancelled")
  void should_admit_later_reservations_when_blocking_one_is_cancelled() {
    var budget = new DownloadBudget(100);
    budget.acquire("a", 60);
    var large = budget.acquire("b", 60);
    var small = budget.acquire("c", 10);
    assertThat(small).isNotDone();

    large.cancel(false);

    assertThat(small).isCompleted();
    assertThat(budget.used()).isEqualTo(70);
  }
}
