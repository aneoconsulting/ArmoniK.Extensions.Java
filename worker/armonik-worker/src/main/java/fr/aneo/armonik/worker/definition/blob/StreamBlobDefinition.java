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
 * Input blob definition backed by an input stream.
 * <p>
 * The stream will be consumed when the blob is created. The caller must ensure the stream
 * remains valid until creation completes.
 * </p>
 * <p>
 * This implementation is suitable for large datasets that should not be loaded entirely
 * into memory. The stream is consumed lazily during blob creation.
 * </p>
 * <p>
 * <strong>Resource Management:</strong> The caller is responsible for closing the stream
 * after blob creation. Use try-with-resources to ensure proper cleanup.
 * </p>
 *
 * @see InputBlobDefinition
 * @see InMemoryBlobDefinition
 */
public final class StreamBlobDefinition implements InputBlobDefinition {
  private final String name;
  private final InputStream stream;

  /**
   * Package-private constructor to enforce factory method usage.
   *
   * @param name   the blob name; must not be {@code null}
   * @param stream the input stream providing blob data; must not be {@code null}
   */
  StreamBlobDefinition(String name, InputStream stream) {
    this.name = requireNonNull(name, "name cannot be null");
    this.stream = requireNonNull(stream, "stream cannot be null");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public InputStream asStream() {
    return stream;
  }

  @Override
  public String toString() {
    return "StreamBlob[name='" + name + "', stream=" + stream.getClass().getSimpleName() + "]";
  }
}

