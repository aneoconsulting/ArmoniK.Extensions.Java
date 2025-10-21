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

import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Blob definition backed by an input stream.
 * <p>
 * The stream will be consumed when the blob is created. The caller must ensure the stream
 * remains valid until creation.
 * </p>
 *
 * @param name   the Result name in ArmoniK; may be {@code null}
 * @param stream the input stream providing blob data; never {@code null}
 */
record StreamBlob(String name, InputStream stream) implements BlobDefinition {

  StreamBlob {
    requireNonNull(stream, "stream cannot be null");
  }
}
