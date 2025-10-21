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
package fr.aneo.armonik.worker.definition.blob;

import static java.util.Objects.requireNonNull;

/**
 * Blob definition backed by an in-memory byte array.
 * <p>
 * The data is already loaded in memory and will be written directly to the shared folder
 * when the blob is created.
 * </p>
 *
 * @param name the Result name in ArmoniK; may be {@code null}
 * @param data the blob data in memory; never {@code null}
 */
record InMemoryBlob(String name, byte[] data) implements BlobDefinition {

  InMemoryBlob {
    requireNonNull(data, "data cannot be null");
  }
}
