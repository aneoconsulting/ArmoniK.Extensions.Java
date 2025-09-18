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
package fr.aneo.armonik.client.blob;

import fr.aneo.armonik.client.session.SessionHandle;

import java.util.UUID;

import static fr.aneo.armonik.client.session.SessionHandleFixture.sessionHandle;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class BlobHandleFixture {

  private BlobHandleFixture() {}

  public static BlobHandle blobHandle() {
    return blobHandle(sessionHandle(), randomUUID());
  }

  public static BlobHandle blobHandle(SessionHandle sessionHandle) {
    return blobHandle(sessionHandle, randomUUID());
  }
  public static BlobHandle blobHandle(UUID id) {
    return blobHandle(sessionHandle(), id);
  }
  public static BlobHandle blobHandle(SessionHandle sessionHandle, UUID id) {
    return new BlobHandle(sessionHandle, completedFuture(new BlobMetadata(id)));
  }
}
