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
package fr.aneo.armonik.worker;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import fr.aneo.armonik.worker.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.definition.task.TaskDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Represents the standard task payload structure in the ArmoniK Java SDK.
 * <p>
 * A {@code Payload} is a special input blob that contains the mapping between logical names
 * and blob IDs for a task's inputs and outputs. This allows task processors to work with
 * meaningful names (e.g., "trainingData", "model") instead of opaque identifiers.
 * </p>
 *
 * <h2>Payload Structure</h2>
 * <p>
 * The payload is serialized as JSON with this structure:
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
 * <p>
 * Where:
 * </p>
 * <ul>
 *   <li><strong>inputs</strong>: Maps logical names to blob IDs of task inputs</li>
 *   <li><strong>outputs</strong>: Maps logical names to blob IDs of task outputs</li>
 * </ul>
 *
 * <h2>Validation Rules</h2>
 * <p>
 * When parsing from JSON, the following validations are performed:
 * </p>
 * <ul>
 *   <li>JSON must not be {@code null}</li>
 *   <li>Root object must contain both "inputs" and "outputs" keys</li>
 *   <li>Both "inputs" and "outputs" values must be non-null objects (can be empty)</li>
 * </ul>
 *
 * <h2>Immutability</h2>
 * <p>
 * This class is immutable. The {@link #inputsMapping()} and {@link #outputsMapping()}
 * methods return defensive copies to prevent external modification.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * {@code Payload} instances are thread-safe due to immutability. The shared
 * {@link Gson} instance is also thread-safe.
 * </p>
 *
 * @see TaskDefinition
 * @see InputBlobDefinition
 * @see BlobService
 */
final class Payload {
  private static final Logger logger = LoggerFactory.getLogger(Payload.class);
  private static final Gson gson = new GsonBuilder()
    .registerTypeAdapter(BlobId.class, blobIdDeserializer())
    .registerTypeAdapter(BlobId.class, blobIdJsonSerializer())
    .create();

  private final Map<String, Map<String, BlobId>> mapping;

  private Payload(Map<String, Map<String, BlobId>> mapping) {
    this.mapping = mapping;
  }

  /**
   * Returns the mapping of logical input names to blob IDs.
   * <p>
   * Each entry represents an input for the task:
   * </p>
   * <ul>
   *   <li><strong>Key</strong>: Logical name (e.g., "trainingData", "config")</li>
   *   <li><strong>Value</strong>: Blob ID as string (e.g., "a7f3e9b2-4c8d-...")</li>
   * </ul>
   * <p>
   * The returned map is an immutable copy. Changes to it do not affect the internal state.
   * </p>
   *
   * @return an immutable map of logical input names to blob IDs; never {@code null},
   *         may be empty if the task has no inputs
   */
  Map<String, BlobId> inputsMapping() {
    return Map.copyOf(mapping.get("inputs"));
  }

  /**
   * Returns the mapping of logical output names to blob IDs.
   * <p>
   * Each entry represents an expected output for the task:
   * </p>
   * <ul>
   *   <li><strong>Key</strong>: Logical name (e.g., "result", "log")</li>
   *   <li><strong>Value</strong>: Blob ID as string (e.g., "e9f5b7c4-3a2d-...")</li>
   * </ul>
   * <p>
   * The returned map is an immutable copy. Changes to it do not affect the internal state.
   * </p>
   *
   * @return an immutable map of logical output names to blob IDs; never {@code null},
   *         may be empty if the task has no outputs
   */
  Map<String, BlobId> outputsMapping() {
    return Map.copyOf(mapping.get("outputs"));
  }

  /**
   * Converts this payload to an input blob definition for task submission.
   * <p>
   * The payload is serialized as JSON and wrapped in an {@link InputBlobDefinition}
   * with the standard name "payload". This blob can then be submitted as a task input.
   * </p>
   * <h4>Usage Example</h4>
   * <pre>{@code
   * var payload = Payload.from(inputIds, outputIds);
   * var payloadBlob = payload.asInputBlobDefinition();
   *
   * taskDefinition.withInput("payload", payloadBlob);
   * }</pre>
   *
   * @return an input blob definition containing the serialized payload; never {@code null}
   * @see InputBlobDefinition
   */
  InputBlobDefinition asInputBlobDefinition() {
    byte[] bytes = gson.toJson(mapping).getBytes(UTF_8);
    return InputBlobDefinition.from("payload", bytes);
  }

  /**
   * Creates a payload from input and output blob ID mappings.
   * <p>
   * This factory method is used on the client side when submitting tasks. It builds
   * the payload structure from the blob IDs of inputs and expected outputs.
   * </p>
   * <p>
   * <strong>Note:</strong> At least one input or one output must be provided. A task
   * cannot have both empty inputs and empty outputs.
   * </p>
   *
   * @param inputs  map of logical names to blob IDs for task inputs; must not be {@code null} or empty
   * @param outputs map of logical names to blob IDs for task outputs; must not be {@code null} or empty
   * @return a new payload instance; never {@code null}
   * @throws NullPointerException     if any parameter is {@code null}
   * @throws IllegalArgumentException if both inputs and outputs are empty
   */
  static Payload from(Map<String, BlobId> inputs, Map<String, BlobId> outputs) {
    requireNonNull(inputs, "inputs cannot be null");
    requireNonNull(outputs, "outputs cannot be null");

    if (inputs.isEmpty() || outputs.isEmpty()) {
      logger.error("Cannot create payload with empty inputs or outputs");
      throw new IllegalArgumentException("Both inputs and outputs must be non-empty");
    }

    var mapping = new HashMap<String, Map<String, BlobId>>();
    mapping.put("inputs", inputs);
    mapping.put("outputs", outputs);

    return new Payload(mapping);
  }

  /**
   * Parses a JSON string into a payload.
   * <p>
   * This factory method is used on the worker side to parse the payload blob received
   * from the task. It deserializes the JSON and validates its structure according to
   * the payload format specification.
   * </p>
   *
   * <h4>Expected JSON Format</h4>
   * <pre>{@code
   * {
   *   "inputs": {
   *     "inputName": "blobId",
   *     ...
   *   },
   *   "outputs": {
   *     "outputName": "blobId",
   *     ...
   *   }
   * }
   * }</pre>
   *
   * <h4>Validation</h4>
   * <p>
   * The method validates:
   * </p>
   * <ul>
   *   <li>JSON string is not {@code null}</li>
   *   <li>Deserialized result is not {@code null}</li>
   *   <li>Root object contains "inputs" key</li>
   *   <li>Root object contains "outputs" key</li>
   *   <li>"inputs" value is not {@code null} (can be empty object)</li>
   *   <li>"outputs" value is not {@code null} (can be empty object)</li>
   * </ul>
   *
   * @param jsonString the JSON string to parse; must not be {@code null}
   * @return a validated payload instance; never {@code null}
   * @throws NullPointerException                if {@code jsonString} is {@code null}
   * @throws IllegalArgumentException            if the JSON structure is invalid or missing required keys
   * @throws com.google.gson.JsonSyntaxException if the JSON is malformed
   */
  static Payload fromJson(String jsonString) {
    requireNonNull(jsonString, "jsonString cannot be null");

    var type = new TypeToken<Map<String, Map<String, BlobId>>>() {
    }.getType();
    Map<String, Map<String, BlobId>> mapping = gson.fromJson(jsonString, type);

    validateMapping(mapping);


    return new Payload(mapping);
  }

  private static void validateMapping(Map<String, Map<String, BlobId>> mapping) {
    if (mapping == null) {
      logger.error("mapping cannot be null or empty");
      throw new IllegalArgumentException("mapping cannot be null or empty");
    }

    if (!mapping.containsKey("inputs")) {
      logger.error("mapping must contain 'inputs' key. Available keys: {}", mapping.keySet());
      throw new IllegalArgumentException("mapping must contain 'inputs' key");
    }

    if (!mapping.containsKey("outputs")) {
      logger.error("mapping must contain 'outputs' key. Available keys: {}", mapping.keySet());
      throw new IllegalArgumentException("mapping must contain 'outputs' key");
    }

    if (mapping.get("inputs") == null) {
      logger.error("'inputs' value cannot be null");
      throw new IllegalArgumentException("'inputs' value cannot be null");
    }

    if (mapping.get("outputs") == null) {
      logger.error("'outputs' value cannot be null");
      throw new IllegalArgumentException("'outputs' value cannot be null");
    }

    logger.debug("Payload validated: {} inputs, {} outputs", mapping.get("inputs").size(), mapping.get("outputs").size());
  }

  @Override
  public String toString() {
    return "Payload[inputs=" + mapping.get("inputs").keySet() +
      ", outputs=" + mapping.get("outputs").keySet() + "]";
  }

  private static JsonDeserializer<BlobId> blobIdDeserializer() {
    return (json, typeOfT, ctx) -> {
      if (json == null || json.isJsonNull()) return null;
      if (json.isJsonPrimitive() && json.getAsJsonPrimitive().isString()) {
        return BlobId.from(json.getAsString());
      }
      throw new JsonParseException("Expected string for BlobId but got: " + json);
    };
  }

  private static JsonSerializer<BlobId> blobIdJsonSerializer() {
    return (src, typeOfSrc, ctx) ->
      src == null ? null : new JsonPrimitive(src.asString());
  }
}
