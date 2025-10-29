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

import fr.aneo.armonik.worker.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.definition.task.TaskDefinition;
import fr.aneo.armonik.worker.internal.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

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
 * <h2>Task Submission</h2>
 * <p>
 * Tasks can submit subtasks during execution. This enables dynamic task graphs where the
 * structure is determined at runtime:
 * </p>
 * <pre>{@code
 * // Create task definition
 * var taskDef = TaskDefinition.builder()
 *   .withInput("data", InputBlobDefinition.from(bytes))
 *   .withOutput("result", OutputBlobDefinition.create())
 *   .withConfiguration(config)
 *   .build();
 *
 * // Submit subtask
 * TaskHandle handle = taskContext.submitTask(taskDef);
 *
 * // Wait for completion if needed
 * handle.waitForCompletion();
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
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe. Multiple threads can access inputs, outputs, and submit tasks
 * concurrently.
 * </p>
 *
 * @see TaskProcessor
 * @see TaskInput
 * @see TaskOutput
 * @see Payload
 * @see TaskDefinition
 */
public class TaskContext {
  private static final Logger logger = LoggerFactory.getLogger(TaskContext.class);

  private final BlobService blobService;
  private final TaskService taskService;
  private final SessionId sessionId;
  private final Map<String, TaskInput> inputs;
  private final Map<String, TaskOutput> outputs;
  private final Queue<CompletionStage<?>> pendingOperations = new ConcurrentLinkedQueue<>();

  /**
   * Creates a task context with full task submission support.
   * <p>
   * This is the primary constructor used by the worker infrastructure. It provides access to
   * all context features including task submission.
   * </p>
   *
   * @param blobService the blob service for managing task data; must not be {@code null}
   * @param taskService the task service for submitting subtasks; may be {@code null} to disable submission
   * @param sessionId   the session identifier; may be {@code null} if taskService is {@code null}
   * @param inputs      map of input names to input tasks; must not be {@code null}
   * @param outputs     map of output names to output tasks; must not be {@code null}
   * @throws NullPointerException if blobService, inputs, or outputs is {@code null}
   */
  TaskContext(BlobService blobService, TaskService taskService, SessionId sessionId, Map<String, TaskInput> inputs, Map<String, TaskOutput> outputs) {
    this.blobService = requireNonNull(blobService, "blobService cannot be null");
    this.taskService = requireNonNull(taskService, "taskService cannot be null");
    this.sessionId = requireNonNull(sessionId, "sessionId cannot be null");
    this.inputs = requireNonNull(inputs, "inputs cannot be null");
    this.outputs = requireNonNull(outputs, "outputs cannot be null");
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


  /**
   * Creates a blob that can be shared across multiple subtasks.
   * <p>
   * This method creates a blob independently of any task submission, allowing the same blob
   * to be referenced by multiple subtasks. This is useful for:
   * </p>
   *
   * @param inputBlobDefinition the definition of the blob to create; must not be {@code null}
   * @return a handle to the created blob; never {@code null}
   * @throws NullPointerException if {@code inputBlobDefinition} is {@code null}
   * @see BlobHandle
   * @see InputBlobDefinition
   */
  public BlobHandle createBlob(InputBlobDefinition inputBlobDefinition) {
    requireNonNull(inputBlobDefinition, "inputBlobDefinition cannot be null");

    logger.debug("Creating shared blob: {}", inputBlobDefinition.name());

    var blobHandle = blobService.createBlob(inputBlobDefinition);
    trackBlobCompletion(blobHandle);

    return blobHandle;
  }

  /**
   * Submits a subtask for execution within the current session.
   * <p>
   * This method creates and submits a new task based on the provided definition. The subtask
   * will execute asynchronously, potentially on a different worker instance.
   * </p>
   *
   * <h4>Task Creation Flow</h4>
   * <ol>
   *   <li>Input blobs are created from the task definition's input definitions</li>
   *   <li>Output blob metadata is prepared for expected outputs</li>
   *   <li>Existing input handles from the definition are merged with newly created inputs</li>
   *   <li>A payload blob is created containing the input/output ID mappings</li>
   *   <li>The task is submitted to the Agent with all dependencies</li>
   * </ol>
   *
   * <h4>Asynchronous Execution</h4>
   * <p>
   * This method returns immediately with a {@link TaskHandle}. The actual task submission
   * completes asynchronously when all input blobs are created and the payload is uploaded.
   * </p>
   *
   * @param taskDefinition the definition of the task to submit; must not be {@code null}
   * @return a handle to track the submitted task; never {@code null}
   * @throws NullPointerException          if {@code taskDefinition} is {@code null}
   * @throws UnsupportedOperationException if this context does not support task submission
   *                                       (created with the 3-argument constructor)
   * @see TaskDefinition
   * @see TaskHandle
   * @see BlobService
   */
  public TaskHandle submitTask(TaskDefinition taskDefinition) {
    requireNonNull(taskDefinition, "taskDefinition cannot be null");

    logger.debug("Submitting task with {} input definitions and {} output definitions",
      taskDefinition.inputDefinitions().size(),
      taskDefinition.outputDefinitions().size());

    var inputHandles = blobService.createBlobs(taskDefinition.inputDefinitions());
    trackBlobCompletions(inputHandles.values());

    var outputBlobHandles = blobService.prepareBlobs(taskDefinition.outputDefinitions());
    trackBlobCompletions(outputBlobHandles.values());

    var allInputHandles = new HashMap<>(inputHandles);
    allInputHandles.putAll(taskDefinition.inputHandles());
    logger.debug("Total inputs after merging: {}", allInputHandles.size());

    var inputIdsByName = getIdsFrom(allInputHandles);
    var outputIdsByName = getIdsFrom(outputBlobHandles);

    var deferredTaskInfo = Futures.allOf(List.of(inputIdsByName, outputIdsByName))
                                  .thenCompose(allIds -> {
                                    var inputIds = allIds.get(0);
                                    var outputIds = allIds.get(1);
                                    logger.debug("Creating payload with input keys:{} and output keys:{}", inputIds.keySet(), outputIds.keySet());

                                    var payload = Payload.from(inputIds, outputIds);
                                    return blobService.createBlob(payload.asInputBlobDefinition())
                                                      .deferredBlobInfo()
                                                      .thenCompose(submitTask(taskDefinition, payload));
                                  })
                                  .whenComplete((taskInfo, throwable) -> {
                                    if (throwable != null) {
                                      logger.error("Task submission failed", throwable);
                                    } else {
                                      logger.debug("Task submitted successfully with id: {}", taskInfo.id());
                                    }
                                  });
    trackTaskCompletion(deferredTaskInfo);

    return new TaskHandle(sessionId, taskDefinition.configuration(), deferredTaskInfo, allInputHandles, outputBlobHandles);
  }

  void awaitCompletion() {
    if (pendingOperations.isEmpty()) {
      logger.debug("No pending operations to wait for");
      return;
    }

    int operationCount = pendingOperations.size();
    logger.info("Waiting for {} pending operations to complete", operationCount);

    try {
      var allOperations = CompletableFuture.allOf(
        pendingOperations.stream()
                         .map(CompletionStage::toCompletableFuture)
                         .toArray(CompletableFuture[]::new)
      );

      allOperations.get();
      logger.info("All {} operations completed successfully", operationCount);

    } catch (ExecutionException e) {
      logger.error("Operation failed during execution", e.getCause());
      throw new ArmoniKException("Operation failed during task execution", e.getCause());

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Interrupted while waiting for operations to complete");
      throw new ArmoniKException("Interrupted while waiting for operations", e);
    }
  }

  private Function<BlobInfo, CompletionStage<TaskInfo>> submitTask(TaskDefinition taskDefinition, Payload payload) {
    return payloadInfo -> {
      logger.debug("Submitting task to Agent with payload id: {}", payloadInfo.id());
      return taskService.submitTask(payload.inputsMapping().values(), payload.outputsMapping().values(), payloadInfo.id(), taskDefinition.configuration());
    };
  }

  private void trackBlobCompletions(Collection<BlobHandle> blobHandles) {
    logger.debug("Tracking blob completion. size={}", blobHandles.size());
    pendingOperations.addAll(blobHandles.stream().map(BlobHandle::deferredBlobInfo).toList());
  }

  private void trackBlobCompletion(BlobHandle blobHandle) {
    logger.debug("Tracking blob completion");
    pendingOperations.add(blobHandle.deferredBlobInfo());
  }

  private void trackTaskCompletion(CompletionStage<TaskInfo> deferredTaskInfo) {
    logger.debug("Tracking task completion");
    pendingOperations.add(deferredTaskInfo);
  }

  /**
   * Extracts blob IDs from a map of blob handles.
   * <p>
   * This method waits for all blob handles to complete and returns a map of logical names
   * to blob IDs. The operation is asynchronous and completes when all blob metadata is
   * available.
   * </p>
   *
   * @param blobHandles map of logical names to blob handles
   * @return a completion stage that yields a map of names to blob IDs
   */
  private static CompletionStage<Map<String, BlobId>> getIdsFrom(Map<String, BlobHandle> blobHandles) {
    return Futures.allOf(mapValues(blobHandles, BlobHandle::deferredBlobInfo))
                  .thenApply(blobInfos -> mapValues(blobInfos, BlobInfo::id));
  }

  /**
   * Transforms the values of a map using the provided function.
   * <p>
   * This is a convenience method for functional-style map transformations.
   * </p>
   *
   * @param map    the source map
   * @param mapper the transformation function
   * @param <K>    the key type
   * @param <V>    the source value type
   * @param <U>    the target value type
   * @return a new map with transformed values
   */
  private static <K, V, U> Map<K, U> mapValues(Map<K, V> map, Function<V, U> mapper) {
    return map.entrySet()
              .stream()
              .collect(toMap(Map.Entry::getKey, entry -> mapper.apply(entry.getValue())));
  }
}
