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
package fr.aneo.armonik.client;

import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.CreateResultsMetaDataRequest;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Test-only access to package-private SDK types, for the integration tests.
 */
public final class ItTestAccess {
  private ItTestAccess() {
  }

  /**
   * Creates, in the session, a blob that is never written, and returns a handle to it. A task taking it
   * as input stays pending forever without using a worker, so its outputs stay watched.
   */
  public static BlobHandle neverWrittenBlob(ArmoniKClient client, SessionHandle session) throws ReflectiveOperationException {
    var field = ArmoniKClient.class.getDeclaredField("channelPool");
    field.setAccessible(true);
    var pool = (ChannelPool) field.get(client);
    var sessionId = session.sessionInfo().id();
    var response = pool.execute(channel -> ResultsGrpc.newBlockingStub(channel)
                                                      .withDeadlineAfter(30, TimeUnit.SECONDS)
                                                      .createResultsMetaData(CreateResultsMetaDataRequest.newBuilder()
                                                                                                        .setSessionId(sessionId.asString())
                                                                                                        .addResults(CreateResultsMetaDataRequest.ResultCreate.newBuilder()
                                                                                                                                                             .setName("never-" + UUID.randomUUID()))
                                                                                                        .build()));
    var raw = response.getResults(0);
    var info = new BlobInfo(BlobId.from(raw.getResultId()), sessionId, raw.getName(), false, null, Instant.now());
    return new BlobHandle(sessionId, java.util.concurrent.CompletableFuture.completedFuture(info), pool);
  }
}
