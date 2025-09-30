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

import fr.aneo.armonik.api.grpc.v1.sessions.SessionsGrpc;
import io.grpc.stub.StreamObserver;

import static fr.aneo.armonik.api.grpc.v1.sessions.SessionsCommon.CreateSessionReply;
import static fr.aneo.armonik.api.grpc.v1.sessions.SessionsCommon.CreateSessionRequest;

public class SessionsGrpcMock extends SessionsGrpc.SessionsImplBase{

  public CreateSessionRequest submittedCreateSessionRequest;

  @Override
  public void createSession(CreateSessionRequest request, StreamObserver<CreateSessionReply> responseObserver) {
    var response = CreateSessionReply.newBuilder().setSessionId("SessionId").build();
    this.submittedCreateSessionRequest = request;
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
