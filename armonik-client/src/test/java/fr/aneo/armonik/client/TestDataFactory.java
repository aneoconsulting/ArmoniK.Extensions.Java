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
package fr.aneo.armonik.client;

import java.time.Instant;
import java.util.Set;

import static fr.aneo.armonik.client.TaskConfiguration.defaultConfiguration;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Mockito.mock;

public class TestDataFactory {
  public static SessionId sessionId() {
    return sessionId("sessionId");
  }

  public static SessionId sessionId(String sessionId) {
    return SessionId.from(sessionId);
  }

  static SessionInfo sessionInfo(String partition) {
    return new SessionInfo(SessionId.from("SessionId"), Set.of(partition), defaultConfiguration());
  }

  public static BlobId blobId(String blobId) {
    return BlobId.from(blobId);
  }

  public static BlobInfo blobInfo(String id) {
    return new BlobInfo(
      blobId(id),
      sessionId("sessionId"),
      "",
      false,
      TaskId.from("taskId"),
      Instant.now()
    );
  }

  public static BlobHandle blobHandle(String sessionId, String blobId) {
    return new BlobHandle(sessionId(sessionId), completedFuture(blobInfo(blobId)), mock(ChannelPool.class));
  }
}
