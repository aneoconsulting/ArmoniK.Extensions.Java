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
package fr.aneo.armonik.worker.domain;

import fr.aneo.armonik.worker.domain.internal.BlobWriter;

import java.nio.file.Path;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Mockito.mock;

public class TestDataFactory {

  public static BlobHandle blobHandle(String blobId) {
    return new BlobHandle(blobId, completedFuture(new BlobInfo(BlobId.from(blobId), BlobStatus.CREATED)));
  }

  public static TaskInput taskInput(String id, String name) {
    return new TaskInput(BlobId.from(id), name, mock(Path.class));
  }

  public static TaskOutput taskOutput(String id, String name) {
    return new TaskOutput(BlobId.from(id), name, mock(BlobWriter.class));
  }
}
