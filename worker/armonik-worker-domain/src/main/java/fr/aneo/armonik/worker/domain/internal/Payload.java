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

import fr.aneo.armonik.worker.domain.BlobId;
import fr.aneo.armonik.worker.domain.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Represents the standard task payload structure in the ArmoniK Java SDK.
 * <p>
 * A {@code Payload} contains the mapping between logical names and blob IDs
 * for a task's inputs and outputs. This allows task processors to work with
 * meaningful names (e.g., "trainingData", "model") instead of opaque identifiers.
 * </p>
 *
 * <h2>Payload Structure</h2>
 * <p>
 * The payload consists of two maps:
 * </p>
 * <ul>
 *   <li><strong>inputs</strong>: Maps logical input names to blob IDs</li>
 *   <li><strong>outputs</strong>: Maps logical output names to blob IDs</li>
 * </ul>
 *
 * <h2>Wire Format</h2>
 * <p>
 * The payload is serialized to JSON using a {@link PayloadSerializer} implementation.
 * The standard JSON format is:
 * </p>
 * <pre>{@code
 * {
 *   "inputs": {
 *     "logicalName1": "blobId1",
 *     "logicalName2": "blobId2"
 *   },
 *   "outputs": {
 *     "logicalName3": "blobId3",
 *     "logicalName4": "blobId4"
 *   }
 * }
 * }</pre>
 *
 * <h2>Usage</h2>
 * <p>
 * On the client side, payloads are created when submitting tasks:
 * </p>
 * <pre>{@code
 * var payload = Payload.from(
 *     Map.of("input1", BlobId.from("blob-id-1")),
 *     Map.of("output1", BlobId.from("blob-id-2"))
 * );
 * }</pre>
 * <p>
 * On the worker side, payloads are deserialized from bytes received from the Agent:
 * </p>
 * <pre>{@code
 * byte[] payloadBytes = readFromFile(payloadPath);
 * Payload payload = payloadSerializer.deserialize(payloadBytes);
 * }</pre>
 *
 * <h2>Immutability</h2>
 * <p>
 * This class is immutable. The {@link #inputsMapping()} and {@link #outputsMapping()}
 * methods return defensive copies to prevent external modification.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * {@code Payload} instances are thread-safe due to immutability.
 * </p>
 *
 * @see PayloadSerializer
 * @see TaskContext
 */
public final class Payload {
  private static final Logger logger = LoggerFactory.getLogger(Payload.class);

  private final Map<String, BlobId> inputs;
  private final Map<String, BlobId> outputs;

  private Payload(Map<String, BlobId> inputs, Map<String, BlobId> outputs) {
    this.inputs = Map.copyOf(inputs);
    this.outputs = Map.copyOf(outputs);
  }

  /**
   * Returns the mapping of logical input names to blob IDs.
   * <p>
   * Each entry represents an input for the task:
   * </p>
   * <ul>
   *   <li><strong>Key</strong>: Logical name (e.g., "trainingData", "config")</li>
   *   <li><strong>Value</strong>: Blob ID (e.g., "a7f3e9b2-4c8d-...")</li>
   * </ul>
   * <p>
   * The returned map is an immutable copy. Changes to it do not affect the internal state.
   * </p>
   *
   * @return an immutable map of logical input names to blob IDs; never {@code null},
   *         may be empty if the task has no inputs
   */
  public Map<String, BlobId> inputsMapping() {
    return inputs;
  }

  /**
   * Returns the mapping of logical output names to blob IDs.
   * <p>
   * Each entry represents an expected output for the task:
   * </p>
   * <ul>
   *   <li><strong>Key</strong>: Logical name (e.g., "result", "log")</li>
   *   <li><strong>Value</strong>: Blob ID (e.g., "e9f5b7c4-3a2d-...")</li>
   * </ul>
   * <p>
   * The returned map is an immutable copy. Changes to it do not affect the internal state.
   * </p>
   *
   * @return an immutable map of logical output names to blob IDs; never {@code null},
   *         may be empty if the task has no outputs
   */
  public Map<String, BlobId> outputsMapping() {
    return outputs;
  }

  /**
   * Creates a payload from input and output blob ID mappings.
   * <p>
   * This factory method is used when submitting tasks. It builds the payload
   * structure from the blob IDs of inputs and expected outputs.
   * </p>
   * <p>
   * <strong>Note:</strong> At least one input or one output must be provided. A task
   * cannot have both empty inputs and empty outputs.
   * </p>
   *
   * @param inputs  map of logical names to blob IDs for task inputs; must not be {@code null}
   * @param outputs map of logical names to blob IDs for task outputs; must not be {@code null}
   * @return a new payload instance; never {@code null}
   * @throws NullPointerException     if any parameter is {@code null}
   * @throws IllegalArgumentException if both inputs and outputs are empty
   */
  public static Payload from(Map<String, BlobId> inputs, Map<String, BlobId> outputs) {
    requireNonNull(inputs, "inputs cannot be null");
    requireNonNull(outputs, "outputs cannot be null");

    if (inputs.isEmpty() || outputs.isEmpty()) {
      logger.error("Cannot create payload with empty inputs or outputs");
      throw new IllegalArgumentException("Both inputs and outputs must be non-empty");
    }

    return new Payload(inputs, outputs);
  }

  @Override
  public String toString() {
    return "Payload[inputs=" + inputs.keySet() +
      ", outputs=" + outputs.keySet() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Payload payload)) return false;
    return inputs.equals(payload.inputs) && outputs.equals(payload.outputs);
  }

  @Override
  public int hashCode() {
    return 31 * inputs.hashCode() + outputs.hashCode();
  }
}
