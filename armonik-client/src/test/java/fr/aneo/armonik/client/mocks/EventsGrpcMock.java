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
package fr.aneo.armonik.client.mocks;

import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionRequest;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse.NewResult;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionResponse.ResultStatusUpdate;
import fr.aneo.armonik.api.grpc.v1.events.EventsGrpc;
import io.grpc.stub.StreamObserver;

import java.util.UUID;

import static fr.aneo.armonik.api.grpc.v1.results.ResultStatusOuterClass.*;

public class EventsGrpcMock extends EventsGrpc.EventsImplBase {
  private StreamObserver<EventSubscriptionResponse> streamObserver;

  @Override
  public void getEvents(EventSubscriptionRequest request, StreamObserver<EventSubscriptionResponse> responseObserver) {
    this.streamObserver = responseObserver;
  }

  public void emitNewResult(UUID blobId, ResultStatus status) {
    streamObserver.onNext(EventSubscriptionResponse.newBuilder()
                                                   .setNewResult(NewResult.newBuilder()
                                                                          .setResultId(blobId.toString())
                                                                          .setStatus(status))
                                                   .build());
  }

  public void emitStatusUpdate(UUID blobId, ResultStatus status) {
    streamObserver.onNext(EventSubscriptionResponse.newBuilder()
                                                   .setResultStatusUpdate(ResultStatusUpdate.newBuilder()
                                                                                            .setResultId(blobId.toString())
                                                                                            .setStatus(status))
                                                   .build());
  }

  public void complete() {
    if (streamObserver != null) {
      streamObserver.onCompleted();
    }
  }
}
