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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Bounds the bytes of downloaded outputs that a client holds at once, overall and per session,
 * without blocking threads.
 * <p>
 * A download reserves the size of its output before it starts, and releases it once the output has
 * been handed over. A reservation that does not fit waits, as a future, until enough bytes are
 * released. Waiting reservations are admitted in order, with two rules:
 * <ul>
 *   <li>a session that has reached its own cap holds back only its own later reservations, so that a
 *       session whose outputs are not drained does not stop the others;</li>
 *   <li>a reservation that does not fit in the overall budget holds back every later one, so that a
 *       large output is not overtaken indefinitely by smaller ones.</li>
 * </ul>
 * A reservation larger than a cap is admitted alone, once nothing else is reserved under that cap,
 * so that it never waits forever. A reservation whose future the caller completes or cancels while it
 * waits leaves the queue without reserving anything.
 */
public final class DownloadBudget {
  private static final DownloadBudget UNLIMITED = new DownloadBudget(Long.MAX_VALUE, Long.MAX_VALUE);

  private final long capacity;
  private final long sessionCapacity;
  private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
  private final Map<Object, Long> sessionUsed = new HashMap<>();
  private long used;

  /**
   * Creates a budget.
   *
   * @param capacity        the bytes all sessions may hold at once, at least 1
   * @param sessionCapacity the bytes one session may hold at once, at least 1
   * @throws IllegalArgumentException if a capacity is below 1
   */
  public DownloadBudget(long capacity, long sessionCapacity) {
    if (capacity < 1) throw new IllegalArgumentException("capacity must be at least 1, got: " + capacity);
    if (sessionCapacity < 1) throw new IllegalArgumentException("sessionCapacity must be at least 1, got: " + sessionCapacity);
    this.capacity = capacity;
    this.sessionCapacity = sessionCapacity;
  }

  /**
   * Returns the budget that admits every reservation at once.
   *
   * @return the unlimited budget
   */
  public static DownloadBudget unlimited() {
    return UNLIMITED;
  }

  /**
   * Tells whether this budget admits every reservation at once, so that sizes need not be known.
   *
   * @return true for the unlimited budget
   */
  public boolean isUnlimited() {
    return capacity == Long.MAX_VALUE && sessionCapacity == Long.MAX_VALUE;
  }

  /**
   * Reserves {@code bytes} for {@code session}.
   *
   * @param session the session the reservation belongs to, compared by equality
   * @param bytes   the bytes to reserve; negative values count as 0
   * @return a future completed once the bytes are reserved, or completed exceptionally by
   *         {@link #cancel(Object, Throwable)}; cancelling it, or completing it otherwise, while it
   *         waits withdraws the reservation
   */
  public CompletableFuture<Void> acquire(Object session, long bytes) {
    requireNonNull(session, "session must not be null");
    if (isUnlimited()) return CompletableFuture.completedFuture(null);
    var waiter = new Waiter(session, Math.max(0, bytes), new CompletableFuture<>());
    List<Waiter> admitted;
    synchronized (this) {
      waiters.add(waiter);
      admitted = admit();
    }
    complete(admitted);
    // A reservation abandoned by its caller may hold back the following ones: withdraw it at once
    waiter.admitted().whenComplete((ignored, abandoned) -> {
      if (abandoned != null) withdraw(waiter);
    });
    return waiter.admitted();
  }

  /**
   * Releases bytes reserved by {@link #acquire(Object, long)}, which admits the reservations they make room for.
   *
   * @param session the session the reservation belongs to
   * @param bytes   the bytes reserved
   */
  public void release(Object session, long bytes) {
    requireNonNull(session, "session must not be null");
    if (isUnlimited()) return;
    var released = Math.max(0, bytes);
    List<Waiter> admitted;
    synchronized (this) {
      used -= released;
      var remaining = sessionUsed.getOrDefault(session, 0L) - released;
      if (remaining > 0) sessionUsed.put(session, remaining);
      else sessionUsed.remove(session);
      admitted = admit();
    }
    complete(admitted);
  }

  /**
   * Fails the reservations of {@code session} still waiting; those already admitted are unaffected.
   *
   * @param session the session
   * @param cause   the failure of the waiting reservations
   */
  public void cancel(Object session, Throwable cause) {
    requireNonNull(session, "session must not be null");
    var cancelled = new ArrayList<Waiter>();
    List<Waiter> admitted;
    synchronized (this) {
      waiters.removeIf(waiter -> {
        if (!waiter.session().equals(session)) return false;
        cancelled.add(waiter);
        return true;
      });
      // A cancelled reservation may have held back the following ones
      admitted = admit();
    }
    cancelled.forEach(waiter -> waiter.admitted().completeExceptionally(cause));
    complete(admitted);
  }

  /**
   * Returns the bytes all sessions may hold at once.
   *
   * @return the overall capacity, {@link Long#MAX_VALUE} when unlimited
   */
  public long capacity() {
    return capacity;
  }

  /**
   * Returns the bytes one session may hold at once.
   *
   * @return the per-session capacity, {@link Long#MAX_VALUE} when unlimited
   */
  public long sessionCapacity() {
    return sessionCapacity;
  }

  /**
   * Returns the bytes reserved at the moment, all sessions together.
   *
   * @return the reserved bytes
   */
  public synchronized long used() {
    return used;
  }

  /**
   * Returns the bytes reserved at the moment by {@code session}.
   *
   * @param session the session
   * @return the bytes it reserved
   */
  public synchronized long used(Object session) {
    return sessionUsed.getOrDefault(session, 0L);
  }

  /**
   * Returns the number of reservations waiting.
   *
   * @return the waiting reservations
   */
  public synchronized int waiting() {
    return waiters.size();
  }

  /** Admits the waiting reservations that fit, in order; called with the lock held. */
  private List<Waiter> admit() {
    var admitted = new ArrayList<Waiter>();
    var heldBack = new HashSet<>();
    for (var iterator = waiters.iterator(); iterator.hasNext(); ) {
      var waiter = iterator.next();
      // Abandoned by its caller, and not yet withdrawn by its completion stage
      if (waiter.admitted().isDone()) {
        iterator.remove();
        continue;
      }
      if (heldBack.contains(waiter.session())) continue;
      var reservedBySession = sessionUsed.getOrDefault(waiter.session(), 0L);
      if (!fits(reservedBySession, waiter.bytes(), sessionCapacity)) {
        // Only this session waits for its own outputs to be drained
        heldBack.add(waiter.session());
        continue;
      }
      // Later reservations do not overtake one that waits for the overall budget
      if (!fits(used, waiter.bytes(), capacity)) break;
      iterator.remove();
      used += waiter.bytes();
      sessionUsed.put(waiter.session(), reservedBySession + waiter.bytes());
      admitted.add(waiter);
    }
    return admitted;
  }

  /**
   * Completes the futures of admitted reservations, outside the lock since completing a future runs its
   * dependent stages; a future its caller completed or cancelled in the meantime gives its bytes back.
   */
  private void complete(List<Waiter> admitted) {
    for (var waiter : admitted) {
      if (!waiter.admitted().complete(null)) release(waiter.session(), waiter.bytes());
    }
  }

  /** Removes a reservation abandoned while it waited, which may let the following ones in. */
  private void withdraw(Waiter waiter) {
    List<Waiter> admitted;
    synchronized (this) {
      if (!waiters.remove(waiter)) return;
      admitted = admit();
    }
    complete(admitted);
  }

  private static boolean fits(long reserved, long bytes, long cap) {
    return reserved == 0 || bytes == 0 || bytes <= cap - reserved;
  }

  private record Waiter(Object session, long bytes, CompletableFuture<Void> admitted) {
  }
}
