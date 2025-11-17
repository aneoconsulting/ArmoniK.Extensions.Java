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

import com.google.protobuf.Duration;
import fr.aneo.armonik.api.grpc.v1.sessions.SessionsGrpc;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Map;

import static fr.aneo.armonik.api.grpc.v1.Objects.TaskOptions;
import static fr.aneo.armonik.api.grpc.v1.sessions.SessionsCommon.*;

public class SessionsGrpcMock extends SessionsGrpc.SessionsImplBase {

  public CreateSessionRequest submittedCreateSessionRequest;

  public GetSessionRequest submittedGetSessionRequest;

  @Override
  public void createSession(CreateSessionRequest request, StreamObserver<CreateSessionReply> responseObserver) {
    this.submittedCreateSessionRequest = request;
    responseObserver.onNext(CreateSessionReply.newBuilder().setSessionId("SessionId").build());
    responseObserver.onCompleted();
  }

  @Override
  public void getSession(GetSessionRequest request, StreamObserver<GetSessionResponse> responseObserver) {
    this.submittedGetSessionRequest = request;

    if (request.getSessionId().equals("does not exist")) {
      responseObserver.onError(io.grpc.Status.NOT_FOUND.withDescription("Session not found").asRuntimeException());
    } else {
      responseObserver.onNext(GetSessionResponse.newBuilder()
                                                .setSession(
                                         SessionRaw.newBuilder()
                                                   .setSessionId(request.getSessionId())
                                                   .addAllPartitionIds(List.of("partition1", "partition2"))
                                                   .setOptions(TaskOptions.newBuilder()
                                                                          .setPartitionId("partition1")
                                                                          .setMaxRetries(2)
                                                                          .setPriority(5)
                                                                          .setMaxDuration(Duration.newBuilder().setSeconds(3600))
                                                                          .putAllOptions(Map.of("option1", "value1")))
                                                   .build())
                                                .build());
    }
    responseObserver.onCompleted();
  }
}
