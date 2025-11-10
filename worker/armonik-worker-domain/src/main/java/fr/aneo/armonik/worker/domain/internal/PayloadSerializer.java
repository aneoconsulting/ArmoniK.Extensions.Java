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
package fr.aneo.armonik.worker.domain.internal;

/**
 * Serializes and deserializes {@link Payload} instances to/from the wire format.
 * <p>
 * The standard implementation uses JSON format as defined by the ArmoniK SDK conventions:
 * </p>
 * <pre>{@code
 * {
 *   "inputs": {
 *     "logicalName": "blobId",
 *     ...
 *   },
 *   "outputs": {
 *     "logicalName": "blobId",
 *     ...
 *   }
 * }
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Implementations must be thread-safe as they may be shared across multiple tasks.
 * </p>
 *
 * @see Payload
 */
public interface PayloadSerializer {

  /**
   * Serializes a payload to bytes suitable for transmission.
   * <p>
   * The serialized format must be compatible with the ArmoniK SDK conventions
   * to ensure interoperability across different language implementations.
   * </p>
   *
   * @param payload the payload to serialize; must not be {@code null}
   * @return the serialized payload as bytes; never {@code null}
   * @throws NullPointerException if {@code payload} is {@code null}
   */
  byte[] serialize(Payload payload);

  /**
   * Deserializes a payload from bytes.
   * <p>
   * This method validates the structure and ensures all required fields are present.
   * </p>
   *
   * @param bytes the serialized payload bytes; must not be {@code null}
   * @return the deserialized payload; never {@code null}
   * @throws NullPointerException     if {@code bytes} is {@code null}
   * @throws IllegalArgumentException if the bytes cannot be deserialized or
   *                                  the structure is invalid
   */
  Payload deserialize(byte[] bytes);
}
