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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;

/**
 * Provides access to task inputs, outputs, and processing context for ArmoniK tasks.
 * <p>
 * A {@code TaskContext} is created for each task execution and serves as the primary interface
 * between the {@link TaskProcessor} and the ArmoniK infrastructure. It manages access to input
 * data (payload and data dependencies), output data (expected outputs), and provides methods
 * for interacting with the Agent.
 * </p>
 *
 * <h2>Data Model</h2>
 * <p>
 * The task context operates on a file-based data model where:
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
 * if (taskContext.hasInput("trainingData")) {
 *     InputTask input = taskContext.getInput("trainingData");
 *     byte[] data = input.rawData();
 * }
 *
 * // Write output data
 * OutputTask output = taskContext.getOutput("model");
 * output.write(modelBytes);
 * }</pre>
 *
 * <h2>File Management</h2>
 * <p>
 * The context manages file I/O through {@link TaskInput} and {@link TaskOutput}:
 * </p>
 * <ul>
 *   <li>{@link TaskInput}: Provides read-only access to input files with various read methods</li>
 *   <li>{@link TaskOutput}: Provides write-only access to output files with notification to Agent</li>
 * </ul>
 * <p>
 * Files are located in the data folder specified in the {@link ProcessRequest}. The context
 * validates that all file paths are within this folder to prevent directory traversal attacks.
 * </p>
 *
 * <h2>Resource Management</h2>
 * <p>
 * The context maintains references to the data folder and file paths but does not manage
 * file lifecycle. Files are created and cleaned up by the Agent infrastructure, not by
 * the context.
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
public class TaskContext {
  private static final Logger logger = LoggerFactory.getLogger(TaskContext.class);

  private final Map<String, TaskInput> inputs;
  private final Map<String, TaskOutput> outputs;

  TaskContext(Map<String, TaskInput> inputs, Map<String, TaskOutput> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  /**
   * Returns an immutable view of all input tasks indexed by their logical names.
   * <p>
   * The returned map contains all inputs defined in the task's payload. Changes to the
   * returned map do not affect the context's internal state.
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
   * returned map do not affect the context's internal state.
   * </p>
   *
   * @return an immutable map of output names to {@link TaskOutput} instances; never {@code null},
   * may be empty if the task has no outputs
   */
  public Map<String, TaskOutput> outputs() {
    return Map.copyOf(outputs);
  }
}
