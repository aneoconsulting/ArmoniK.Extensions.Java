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

import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;

import static fr.aneo.armonik.worker.internal.PathValidator.resolveWithin;
import static fr.aneo.armonik.worker.internal.PathValidator.validateFile;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Provides access to task inputs, outputs, and processing context for ArmoniK tasks.
 * <p>
 * A {@code TaskHandler} is created for each task execution and serves as the primary interface
 * between the {@link TaskProcessor} and the ArmoniK infrastructure. It manages access to input
 * data (payload and data dependencies), output data (expected outputs), and provides methods
 * for interacting with the Agent.
 * </p>
 *
 * <h2>Data Model</h2>
 * <p>
 * The task handler operates on a file-based data model where:
 * </p>
 * <ul>
 *   <li><strong>Payload</strong>: A JSON file mapping logical names to blob IDs for inputs and outputs</li>
 *   <li><strong>Inputs</strong>: Files in the shared data folder containing task input data</li>
 *   <li><strong>Outputs</strong>: File paths where task results should be written</li>
 *   <li><strong>Data Folder</strong>: A shared directory accessible by both Agent and Worker</li>
 * </ul>
 *
 * <h2>Input/Output Access</h2>
 * <p>
 * Inputs and outputs are accessed by their logical names defined in the payload:
 * </p>
 * <pre>{@code
 * // Read input data
 * if (taskHandler.hasInput("trainingData")) {
 *     InputTask input = taskHandler.getInput("trainingData");
 *     byte[] data = input.rawData();
 * }
 *
 * // Write output data
 * OutputTask output = taskHandler.getOutput("model");
 * output.write(modelBytes);
 * }</pre>
 *
 * <h2>File Management</h2>
 * <p>
 * The handler manages file I/O through {@link TaskInput} and {@link TaskOutput}:
 * </p>
 * <ul>
 *   <li>{@link TaskInput}: Provides read-only access to input files with various read methods</li>
 *   <li>{@link TaskOutput}: Provides write-only access to output files with notification to Agent</li>
 * </ul>
 * <p>
 * Files are located in the data folder specified in the {@link ProcessRequest}. The handler
 * validates that all file paths are within this folder to prevent directory traversal attacks.
 * </p>
 *
 * <h2>Resource Management</h2>
 * <p>
 * The handler maintains references to the data folder and file paths but does not manage
 * file lifecycle. Files are created and cleaned up by the Agent infrastructure, not by
 * the handler.
 * </p>
 *
 * <h2>Error Handling</h2>
 * <p>
 * Operations that fail due to I/O errors, missing files, or invalid data throw
 * {@link ArmoniKException}. These exceptions should be caught by the {@link TaskProcessor}
 * and converted to appropriate {@link TaskOutcome} values.
 * </p>
 *
 * @see TaskProcessor
 * @see TaskInput
 * @see TaskOutput
 * @see BlobsMapping
 */
public class TaskHandler {
  private static final Logger logger = LoggerFactory.getLogger(TaskHandler.class);

  private final Map<String, TaskInput> inputs;
  private final Map<String, TaskOutput> outputs;

  private TaskHandler(Map<String, TaskInput> inputs, Map<String, TaskOutput> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  /**
   * Returns an immutable view of all input tasks indexed by their logical names.
   * <p>
   * The returned map contains all inputs defined in the task's payload. Changes to the
   * returned map do not affect the handler's internal state.
   * </p>
   *
   * @return an immutable map of input names to {@link TaskInput} instances; never {@code null},
   * may be empty if the task has no inputs
   */
  public Map<String, TaskInput> inputs() {
    return Map.copyOf(inputs);
  }

  /**
   * Checks whether an input with the specified logical name exists.
   * <p>
   * This method should be used to safely check for inputs before calling
   * {@link #getInput(String)}.
   * </p>
   *
   * @param name the logical name of the input to check; may be {@code null}
   * @return {@code true} if an input with the specified name exists, {@code false} otherwise
   */
  public boolean hasInput(String name) {
    return inputs.containsKey(name);
  }

  /**
   * Retrieves the input task with the specified logical name.
   * <p>
   * The returned {@link TaskInput} provides access to the input file contents through
   * various read methods (bytes, string, stream, etc.).
   * </p>
   *
   * @param name the logical name of the input to retrieve; must not be {@code null} or empty
   * @return the input task corresponding to the specified name; never {@code null}
   * @throws IllegalArgumentException if {@code name} is {@code null}, empty, or no input
   *                                  with that name exists
   */
  public TaskInput getInput(String name) {
    if (name == null || name.isEmpty()) throw new IllegalArgumentException("Input name cannot be null or empty");
    if (!hasInput(name)) {
      logger.error("Input not found: '{}'. Available inputs: {}", name, inputs.keySet());
      throw new IllegalArgumentException("Input with name '" + name + "' not found");
    }

    return inputs.get(name);
  }

  /**
   * Checks whether an output with the specified logical name exists.
   * <p>
   * This method should be used to safely check for outputs before calling
   * {@link #getOutput(String)}.
   * </p>
   *
   * @param name the logical name of the output to check; may be {@code null}
   * @return {@code true} if an output with the specified name exists, {@code false} otherwise
   */
  public boolean hasOutput(String name) {
    return outputs.containsKey(name);
  }

  /**
   * Retrieves the output task with the specified logical name.
   * <p>
   * The returned {@link TaskOutput} provides methods to write data to the output file.
   * When data is written, the Agent is automatically notified that the output is ready.
   * </p>
   *
   * @param name the logical name of the output to retrieve; must not be {@code null} or empty
   * @return the output task corresponding to the specified name; never {@code null}
   * @throws IllegalArgumentException if {@code name} is {@code null}, empty, or no output
   *                                  with that name exists
   */
  public TaskOutput getOutput(String name) {
    if (name == null || name.isEmpty()) throw new IllegalArgumentException("Output name cannot be null or empty");
    if (!hasOutput(name)) {
      logger.error("Output not found: '{}'. Available outputs: {}", name, outputs.keySet());
      throw new IllegalArgumentException("Output with name '" + name + "' not found");
    }

    return outputs.get(name);
  }

  /**
   * Returns an immutable view of all output tasks indexed by their logical names.
   * <p>
   * The returned map contains all outputs defined in the task's payload. Changes to the
   * returned map do not affect the handler's internal state.
   * </p>
   *
   * @return an immutable map of output names to {@link TaskOutput} instances; never {@code null},
   * may be empty if the task has no outputs
   */
  public Map<String, TaskOutput> outputs() {
    return Map.copyOf(outputs);
  }

  /**
   * Creates a new task handler from an Agent request.
   * <p>
   * This is the standard factory method used by {@link WorkerGrpc} to create task handlers
   * for each incoming task. The method:
   * </p>
   * <ol>
   *   <li>Resolves the data folder path from the request</li>
   *   <li>Reads and parses the payload file containing input/output mappings</li>
   *   <li>Creates {@link TaskInput} instances for all inputs, validating file existence</li>
   *   <li>Creates {@link TaskOutput} instances for all outputs with Agent notification</li>
   *   <li>Returns a fully initialized task handler</li>
   * </ol>
   *
   * <h3>Validation</h3>
   * <p>
   * The method performs several validation checks:
   * </p>
   * <ul>
   *   <li>All file paths must be within the data folder (prevents directory traversal)</li>
   *   <li>All input files must exist and be readable</li>
   *   <li>The payload file must exist and contain valid JSON</li>
   *   <li>The payload JSON must conform to the expected structure</li>
   * </ul>
   *
   * <h3>Error Cases</h3>
   * <p>
   * This method throws {@link ArmoniKException} if:
   * </p>
   * <ul>
   *   <li>The payload file cannot be read ({@link IOException})</li>
   *   <li>The payload contains invalid JSON ({@link JsonParseException})</li>
   *   <li>The payload JSON structure is incorrect ({@link IllegalArgumentException})</li>
   *   <li>Any input file does not exist or is not readable</li>
   *   <li>Any file path attempts directory traversal</li>
   * </ul>
   *
   * @param agentStub the gRPC stub for communicating with the Agent; used to notify
   *                  the Agent when outputs are ready; must not be {@code null}
   * @param request   the task processing request from the Agent containing the data folder path,
   *                  payload ID, session ID, and communication token; must not be {@code null}
   * @return a fully initialized task handler with access to all inputs and outputs;
   * never {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   * @throws ArmoniKException     if payload reading fails, JSON parsing fails, payload structure
   *                              is invalid, input files are missing, or path validation fails
   */
  static TaskHandler from(AgentFutureStub agentStub, ProcessRequest request) {
    requireNonNull(agentStub, "agentStub");
    requireNonNull(request, "request");

    var dataFolderPath = Path.of(request.getDataFolder());
    var blobsMapping = createBlobsMapping(dataFolderPath, request.getPayloadId());
    var inputs = createInputs(blobsMapping, dataFolderPath);
    var outputs = createOutputs(blobsMapping, dataFolderPath, new AgentNotifier(agentStub, request.getSessionId(), request.getCommunicationToken()));
    return new TaskHandler(inputs, outputs);
  }


  private static Map<String, TaskInput> createInputs(BlobsMapping blobsMapping, Path dataFolderPath) {
    return blobsMapping.inputsMapping()
                       .entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, createInputTask(dataFolderPath)));
  }

  private static Function<Map.Entry<String, String>, TaskInput> createInputTask(Path dataFolderPath) {
    return entry -> {
      String logicalName = entry.getKey();
      String blobId = entry.getValue();
      try {
        var inputFilePath = resolveWithin(dataFolderPath, blobId);
        validateFile(inputFilePath);
        long fileSize = Files.size(inputFilePath);
        logger.info("Input task created: logicalName='{}', blobId={}, size={} bytes", logicalName, blobId, fileSize);
        return new TaskInput(BlobId.from(entry.getValue()), logicalName, inputFilePath);

      } catch (Exception e) {
        logger.error("Failed to create input task: logicalName='{}', blobId={}", logicalName, blobId, e);
        throw new ArmoniKException("Failed to create input task for: " + logicalName, e);
      }
    };
  }

  private static Map<String, TaskOutput> createOutputs(BlobsMapping blobsMapping, Path dataFolderPath, BlobListener listener) {
    return blobsMapping.outputsMapping()
                       .entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, createOutputTask(dataFolderPath, listener)));
  }

  private static Function<Map.Entry<String, String>, TaskOutput> createOutputTask(Path dataFolderPath, BlobListener listener) {
    return entry -> {
      String logicalName = entry.getKey();
      String blobId = entry.getValue();
      try {
        logger.info("Output task created: logicalName='{}', blobId={}", logicalName, blobId);
        return new TaskOutput(BlobId.from(blobId), logicalName, resolveWithin(dataFolderPath, blobId), listener);
      } catch (Exception e) {
        logger.error("Failed to create output task: logicalName='{}', blobId={}", logicalName, blobId, e);
        throw new ArmoniKException("Failed to create output task for: " + entry.getKey(), e);
      }
    };
  }

  private static BlobsMapping createBlobsMapping(Path dataFolderPath, String payloadId) {
    var payload = resolveWithin(dataFolderPath, payloadId);
    validateFile(payload);
    try {
      var payloadData = Files.readString(payload);
      return BlobsMapping.fromJson(payloadData);
    } catch (IOException exception) {
      logger.error("Failed to read payload file: blobId={}, dataFolder={}", payloadId, dataFolderPath, exception);
      throw new ArmoniKException("Failed to read payload file: " + payloadId, exception);
    } catch (JsonParseException exception) {
      logger.error("Payload contains invalid JSON: blobId={}", payloadId, exception);
      throw new ArmoniKException("Payload contains invalid JSON: " + payloadId, exception);
    } catch (IllegalArgumentException exception) {
      logger.error("Payload JSON structure is incorrect: blobId={}, error={}", payloadId, exception.getMessage());
      throw new ArmoniKException("Payload JSON structure is incorrect: " + payloadId, exception);
    }
  }
}
