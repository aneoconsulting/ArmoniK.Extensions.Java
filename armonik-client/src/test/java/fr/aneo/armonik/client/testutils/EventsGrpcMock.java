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
package fr.aneo.armonik.client.testutils;

import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionRequest;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse;
import fr.aneo.armonik.api.grpc.v1.events.EventsGrpc;
import fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Serves {@code GetEvents} as the control plane does for a subscription filtered on result ids: it
 * registers the subscription, sends the current status of every filtered result as a new-result
 * event, then every status set on {@link ResultsGrpcMock} as a status update. Like the control plane,
 * it never ends a subscription on its own.
 */
public class EventsGrpcMock extends EventsGrpc.EventsImplBase {
  private final ResultsGrpcMock results;
  public final List<EventSubscriptionRequest> requests = new CopyOnWriteArrayList<>();
  private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
  /** Number of the next subscriptions that fail at once with {@link #failingStatus}. */
  public final AtomicInteger failingSubscriptions = new AtomicInteger();
  public volatile Status failingStatus = Status.UNAVAILABLE;
  public final AtomicInteger cancelledSubscriptions = new AtomicInteger();

  public EventsGrpcMock(ResultsGrpcMock results) {
    this.results = results;
    results.addStatusListener(this::publish);
  }

  public void reset() {
    requests.clear();
    subscriptions.clear();
    failingSubscriptions.set(0);
    failingStatus = Status.UNAVAILABLE;
    cancelledSubscriptions.set(0);
  }

  /** Number of subscriptions still open. */
  public int openSubscriptions() {
    return subscriptions.size();
  }

  /** Result ids of the filter of a request. */
  public static Set<String> ids(EventSubscriptionRequest request) {
    return request.getResultsFilters().getOrList().stream()
                  .flatMap(and -> and.getAndList().stream())
                  .map(field -> field.getFilterString().getValue())
                  .collect(Collectors.toSet());
  }

  /** Ends every open subscription with {@code status}, as a control plane restart would. */
  public void failOpenSubscriptions(Status status) {
    for (var subscription : subscriptions) {
      subscriptions.remove(subscription);
      synchronized (subscription.observer) {
        try {
          subscription.observer.onError(status.asRuntimeException());
        } catch (RuntimeException ignored) {
          // Already cancelled by the client
        }
      }
    }
  }

  @Override
  public void getEvents(EventSubscriptionRequest request, StreamObserver<EventSubscriptionResponse> responseObserver) {
    requests.add(request);
    if (failingSubscriptions.getAndUpdate(remaining -> Math.max(remaining - 1, 0)) > 0) {
      responseObserver.onError(failingStatus.asRuntimeException());
      return;
    }
    var observer = (ServerCallStreamObserver<EventSubscriptionResponse>) responseObserver;
    var subscription = new Subscription(ids(request), observer);
    observer.setOnCancelHandler(() -> {
      if (subscriptions.remove(subscription)) cancelledSubscriptions.incrementAndGet();
    });
    // Registered before the current statuses are read, as the control plane does, so that no change is missed
    subscriptions.add(subscription);
    for (var id : subscription.ids) {
      var status = results.statuses.getOrDefault(id, ResultStatus.RESULT_STATUS_CREATED);
      send(subscription, EventSubscriptionResponse.newBuilder()
                                                  .setSessionId(request.getSessionId())
                                                  .setNewResult(EventSubscriptionResponse.NewResult.newBuilder().setResultId(id).setStatus(status))
                                                  .build());
    }
  }

  private void publish(String id, ResultStatus status) {
    for (var subscription : subscriptions) {
      if (!subscription.ids.contains(id)) continue;
      send(subscription, EventSubscriptionResponse.newBuilder()
                                                  .setResultStatusUpdate(EventSubscriptionResponse.ResultStatusUpdate.newBuilder().setResultId(id).setStatus(status))
                                                  .build());
    }
  }

  private static void send(Subscription subscription, EventSubscriptionResponse response) {
    synchronized (subscription.observer) {
      try {
        subscription.observer.onNext(response);
      } catch (RuntimeException ignored) {
        // Cancelled by the client meanwhile
      }
    }
  }

  private record Subscription(Set<String> ids, ServerCallStreamObserver<EventSubscriptionResponse> observer) {
  }
}
