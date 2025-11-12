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
package fr.aneo.armonik.worker.internal;

import com.google.gson.*;
import fr.aneo.armonik.worker.domain.BlobId;
import fr.aneo.armonik.worker.domain.internal.Payload;
import fr.aneo.armonik.worker.domain.internal.PayloadSerializer;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * JSON-based implementation of {@link PayloadSerializer} using Gson.
 * <p>
 * This serializer converts {@link Payload} instances to and from JSON format
 * following the ArmoniK SDK conventions:
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
 * <h2>Serialization Details</h2>
 * <p>
 * The serializer:
 * </p>
 * <ul>
 *   <li>Converts {@link BlobId} to plain strings in JSON</li>
 *   <li>Uses UTF-8 encoding for byte conversion</li>
 *   <li>Validates that both inputs and outputs are present during deserialization</li>
 *   <li>Throws {@link IllegalArgumentException} for malformed JSON</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe. The internal Gson instance is immutable and can be
 * safely shared across multiple threads.
 * </p>
 *
 * <h2>JSON Structure</h2>
 * <p>
 * The JSON must contain both {@code inputs} and {@code outputs} objects. Each object
 * contains key-value pairs where:
 * </p>
 * <ul>
 *   <li><strong>Key:</strong> Logical name (string)</li>
 *   <li><strong>Value:</strong> Blob ID (string)</li>
 * </ul>
 *
 * @see Payload
 * @see PayloadSerializer
 * @see BlobId
 */
public final class GsonPayloadSerializer implements PayloadSerializer {

  private final Gson gson;

  /**
   * Creates a new Gson-based payload serializer with default configuration.
   * <p>
   * The Gson instance is configured with a custom {@link BlobId} adapter to handle
   * serialization and deserialization of blob identifiers.
   * </p>
   */
  public GsonPayloadSerializer() {
    this.gson = new GsonBuilder()
      .registerTypeAdapter(BlobId.class, new BlobIdAdapter())
      .create();
  }

  /**
   * Serializes a payload to JSON bytes using UTF-8 encoding.
   * <p>
   * The resulting JSON structure follows the ArmoniK SDK conventions with
   * {@code inputs} and {@code outputs} objects.
   * </p>
   *
   * <h4>Example Output</h4>
   * <pre>{@code
   * {
   *   "inputs": {"data": "blob-123", "config": "blob-456"},
   *   "outputs": {"result": "blob-789"}
   * }
   * }</pre>
   *
   * @param payload the payload to serialize; must not be {@code null}
   * @return the serialized payload as UTF-8 encoded JSON bytes; never {@code null}
   * @throws NullPointerException if {@code payload} is {@code null}
   * @throws JsonIOException      if there was a problem writing to the JSON output
   */
  @Override
  public byte[] serialize(Payload payload) {
    requireNonNull(payload, "payload cannot be null");

    String json = gson.toJson(payload);
    return json.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Deserializes a payload from JSON bytes.
   * <p>
   * This method validates that the JSON structure is correct and contains both
   * {@code inputs} and {@code outputs} objects. Missing or malformed data results
   * in an {@link IllegalArgumentException}.
   * </p>
   *
   * <h4>Validation Rules</h4>
   * <ul>
   *   <li>JSON must be valid UTF-8 encoded text</li>
   *   <li>Must contain {@code inputs} object (may be empty)</li>
   *   <li>Must contain {@code outputs} object (may be empty)</li>
   *   <li>All blob IDs must be valid strings</li>
   * </ul>
   *
   * @param bytes the JSON bytes to deserialize; must not be {@code null}
   * @return the deserialized payload; never {@code null}
   * @throws NullPointerException     if {@code bytes} is {@code null}
   * @throws IllegalArgumentException if the JSON structure is invalid or missing required fields
   * @throws JsonSyntaxException      if the JSON is malformed
   */
  @Override
  public Payload deserialize(byte[] bytes) {
    requireNonNull(bytes, "bytes cannot be null");

    try {
      var json = new String(bytes, StandardCharsets.UTF_8);
      var root = JsonParser.parseString(json).getAsJsonObject();

      // Validate structure
      if (!root.has("inputs") || !root.get("inputs").isJsonObject()) {
        throw new IllegalArgumentException("Payload JSON must contain 'inputs' object");
      }
      if (!root.has("outputs") || !root.get("outputs").isJsonObject()) {
        throw new IllegalArgumentException("Payload JSON must contain 'outputs' object");
      }

      // Deserialize inputs and outputs
      var inputs = deserializeMapping(root.getAsJsonObject("inputs"));
      var outputs = deserializeMapping(root.getAsJsonObject("outputs"));

      return Payload.from(inputs, outputs);

    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException("Invalid JSON format for payload", e);
    } catch (IllegalStateException e) {
      throw new IllegalArgumentException("Malformed JSON structure in payload", e);
    }
  }

  /**
   * Deserializes a JSON object into a map of logical names to blob IDs.
   *
   * @param jsonObject the JSON object containing name-to-ID mappings
   * @return a map of logical names to blob IDs; never {@code null}, may be empty
   * @throws IllegalArgumentException if any value is not a valid string
   */
  private Map<String, BlobId> deserializeMapping(JsonObject jsonObject) {
    return jsonObject.entrySet()
                     .stream()
                     .collect(toMap(
                       Map.Entry::getKey,
                       entry -> {
                         var value = entry.getValue();
                         if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()){
                           throw new IllegalArgumentException("Blob ID for '" + entry.getKey() + "' must be a string, got: " + value);
                         }

                         return BlobId.from(value.getAsString());
                       }
                     ));
  }

  /**
   * Gson adapter for serializing and deserializing {@link BlobId} instances.
   * <p>
   * This adapter converts blob IDs to plain strings in JSON and back to
   * {@link BlobId} instances during deserialization.
   * </p>
   */
  private static final class BlobIdAdapter implements JsonSerializer<BlobId>, JsonDeserializer<BlobId> {

    @Override
    public JsonElement serialize(BlobId src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(src.asString());
    }

    @Override
    public BlobId deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
      if (!json.isJsonPrimitive() || !json.getAsJsonPrimitive().isString()) {
        throw new JsonParseException("BlobId must be a string");
      }
      return BlobId.from(json.getAsString());
    }
  }
}
