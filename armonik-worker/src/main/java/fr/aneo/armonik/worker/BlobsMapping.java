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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Mapping of logical names to blob IDs for task inputs and outputs.
 * <p>
 * A {@code BlobsMapping} is created by parsing the task's payload JSON file, which defines
 * the association between user-friendly logical names (e.g., "trainingData", "model") and
 * internal blob IDs (e.g., UUID strings). This mapping allows task processors to work with
 * meaningful names instead of opaque identifiers.
 * </p>
 *
 * <h2>Payload Structure</h2>
 * <p>
 * The payload JSON must follow this structure:
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
 * <h2>Validation Rules</h2>
 * <p>
 * The following validations are performed during parsing:
 * </p>
 * <ul>
 *   <li>JSON must not be {@code null} or empty</li>
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
 * {@code BlobsMapping} instances are thread-safe due to immutability. The shared
 * {@link Gson} instance is also thread-safe.
 * </p>
 *
 * @see TaskHandler
 * @see TaskInput
 * @see TaskOutput
 */
public class BlobsMapping {
  private static final Gson gson = new Gson();

  private final Map<String, Map<String, String>> mapping;

  private BlobsMapping(Map<String, Map<String, String>> mapping) {
    this.mapping = mapping;
  }

  /**
   * Returns the mapping of logical input names to blob IDs.
   * <p>
   * Each entry in the returned map represents an input for the task:
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
  Map<String, String> inputsMapping() {
    return Map.copyOf(mapping.get("inputs"));
  }

  /**
   * Returns the mapping of logical output names to blob IDs.
   * <p>
   * Each entry in the returned map represents an expected output for the task:
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
  Map<String, String> outputsMapping() {
    return Map.copyOf(mapping.get("outputs"));
  }

  /**
   * Parses a JSON string into a blobs mapping.
   * <p>
   * This method deserializes the payload JSON and validates its structure according to
   * the rules defined in the class documentation. If validation fails, an
   * {@link IllegalArgumentException} is thrown with a descriptive error message.
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
   * @return a validated blobs mapping; never {@code null}
   * @throws NullPointerException if {@code jsonString} is {@code null}
   * @throws IllegalArgumentException if the JSON structure is invalid or missing required keys
   * @throws com.google.gson.JsonSyntaxException if the JSON is malformed
   */
  static BlobsMapping fromJson(String jsonString) {
    requireNonNull(jsonString, "jsonString cannot be null");

    var type = new TypeToken<Map<String, Map<String, String>>>() {
    }.getType();
    Map<String, Map<String, String>> mapping = gson.fromJson(jsonString, type);

    validateMapping(mapping);

    return new BlobsMapping(mapping);
  }

  private static void validateMapping(Map<String, Map<String, String>> mapping) {
    if (mapping == null) {
      throw new IllegalArgumentException("BlobsMapping JSON cannot be null or empty");
    }

    if (!mapping.containsKey("inputs")) {
      throw new IllegalArgumentException("BlobsMapping JSON must contain 'inputs' key");
    }

    if (!mapping.containsKey("outputs")) {
      throw new IllegalArgumentException("BlobsMapping JSON must contain 'outputs' key");
    }

    if (mapping.get("inputs") == null) {
      throw new IllegalArgumentException("BlobsMapping 'inputs' value cannot be null");
    }

    if (mapping.get("outputs") == null) {
      throw new IllegalArgumentException("BlobsMapping 'outputs' value cannot be null");
    }
  }
}
