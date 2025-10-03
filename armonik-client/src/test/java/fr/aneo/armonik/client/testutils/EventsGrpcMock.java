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
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse.NewResult;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse.ResultStatusUpdate;
import fr.aneo.armonik.api.grpc.v1.events.EventsGrpc;
import fr.aneo.armonik.client.model.BlobId;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.ResultStatus;

public class EventsGrpcMock extends EventsGrpc.EventsImplBase {
  private final List<StreamObserver<EventSubscriptionResponse>> observers = new CopyOnWriteArrayList<>();
  private final List<EventSubscriptionResponse> backlog = new CopyOnWriteArrayList<>();
  private volatile boolean completed = false;
  private volatile Throwable terminalError = null;

  @Override
  public void getEvents(EventSubscriptionRequest request,
                        StreamObserver<EventSubscriptionResponse> responseObserver) {
    observers.add(responseObserver);

    backlog.forEach(responseObserver::onNext);

    if (terminalError != null) {
      try {
        responseObserver.onError(terminalError);
      } catch (Throwable ignored) {
      }
    } else if (completed) {
      try {
        responseObserver.onCompleted();
      } catch (Throwable ignored) {
      }
    }
  }

  /**
   * Broadcast a NEW_RESULT event to all subscribers.
   */
  public void emitNewResult(BlobId blobId, ResultStatus status) {
    final var response = EventSubscriptionResponse.newBuilder()
                                                  .setNewResult(NewResult.newBuilder()
                                                                         .setResultId(blobId.asString())
                                                                         .setStatus(status))
                                                  .build();
    broadcast(response);
  }

  public void emitStatusUpdate(BlobId blobId, ResultStatus status) {
    final var response = EventSubscriptionResponse.newBuilder()
                                                  .setResultStatusUpdate(ResultStatusUpdate.newBuilder()
                                                                                           .setResultId(blobId.asString())
                                                                                           .setStatus(status))
                                                  .build();
    broadcast(response);
  }

  public void complete() {
    completed = true;
    terminalError = null;
    observers.forEach(observer -> {
      try {
        observer.onCompleted();
      } catch (Throwable ignored) {
      }
    });

    observers.clear();
  }

  public void error(RuntimeException exception) {
    terminalError = exception;
    observers.forEach(observer -> {
      try {
        observer.onError(exception);
      } catch (Throwable ignored) {
      }
    });

    observers.clear();
  }

  private void broadcast(EventSubscriptionResponse resp) {
    backlog.add(resp);
    observers.forEach(observer -> observer.onNext(resp));
  }
}
