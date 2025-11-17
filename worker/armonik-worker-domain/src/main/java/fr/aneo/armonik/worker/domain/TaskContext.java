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
package fr.aneo.armonik.worker.domain;

import fr.aneo.armonik.worker.domain.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.blob.OutputBlobDefinition;
import fr.aneo.armonik.worker.domain.definition.task.TaskDefinition;
import fr.aneo.armonik.worker.domain.internal.BlobService;
import fr.aneo.armonik.worker.domain.internal.Payload;
import fr.aneo.armonik.worker.domain.internal.PayloadSerializer;
import fr.aneo.armonik.worker.domain.internal.TaskService;
import fr.aneo.armonik.worker.domain.internal.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Execution context provided to {@link TaskProcessor} implementations during task processing.
 * <p>
 * The {@code TaskContext} provides access to:
 * </p>
 * <ul>
 *   <li><strong>Task inputs</strong>: Read-only access to input data via {@link #getInput(String)}</li>
 *   <li><strong>Task outputs</strong>: Write access to expected outputs via {@link #getOutput(String)}</li>
 *   <li><strong>Subtask submission</strong>: Create and submit child tasks via {@link #submitTask(TaskDefinition)}</li>
 *   <li><strong>Shared blob creation</strong>: Create blobs for reuse across subtasks via {@link #createBlob(InputBlobDefinition)}</li>
 * </ul>
 *
 * <h2>Subtask Input Sources</h2>
 * <p>
 * When submitting subtasks, inputs can come from three sources:
 * </p>
 * <ul>
 *   <li><strong>{@link InputBlobDefinition}</strong> - New data to be uploaded during submission</li>
 *   <li><strong>{@link BlobHandle}</strong> - Reference to existing blobs in the cluster</li>
 *   <li><strong>{@link TaskInput}</strong> - Reuse of the parent task's input data (no upload)</li>
 * </ul>
 *
 * <h2>Subtask Output Types</h2>
 * <p>
 * Subtasks can produce outputs in two ways:
 * </p>
 * <ul>
 *   <li><strong>{@link OutputBlobDefinition}</strong> - New outputs the subtask will create</li>
 *   <li><strong>{@link TaskOutput}</strong> - Delegation of the parent's output to the subtask</li>
 * </ul>
 *
 * <h2>Output Delegation Pattern</h2>
 * <p>
 * A parent task can delegate the responsibility of producing one of its expected outputs
 * to a subtask, enabling dynamic task graphs:
 * </p>
 * <pre>{@code
 * // Parent task context
 * TaskOutput resultOutput = taskContext.getOutput("result");
 * TaskInput configInput = taskContext.getInput("config");
 *
 * // Create subtask that produces parent's output
 * var subtask = new TaskDefinition()
 *     .withInput("config", configInput)      // Reuse parent's input
 *     .withOutput("result", resultOutput)    // Subtask produces parent's output
 *     .withConfiguration(subtaskConfig);
 *
 * taskContext.submitTask(subtask);
 * // Parent doesn't write to resultOutput - subtask will produce it
 * }</pre>
 *
 * <h2>Asynchronous Operations</h2>
 * <p>
 * Task submission and blob creation are asynchronous. The {@link #awaitCompletion()} method,
 * called after {@link TaskProcessor#processTask(TaskContext)} returns, ensures all pending operations complete
 * before the task is marked as finished.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is NOT thread-safe. It is designed to be used by a single thread during
 * task processing. The Worker processes one task at a time.
 * </p>
 *
 * @see TaskProcessor
 * @see TaskDefinition
 * @see TaskInput
 * @see TaskOutput
 */
public class TaskContext {
  private static final Logger logger = LoggerFactory.getLogger(TaskContext.class);

  private final BlobService blobService;
  private final TaskService taskService;
  private final PayloadSerializer payloadSerializer;
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
   * @param blobService       the blob service for managing task data; must not be {@code null}
   * @param taskService       the task service for submitting subtasks; must not be {@code null}
   * @param payloadSerializer the serializer for payload encoding/decoding; must not be {@code null}
   * @param sessionId         the session identifier; must not be {@code null}
   * @param inputs            map of input names to input tasks; must not be {@code null}
   * @param outputs           map of output names to output tasks; must not be {@code null}
   * @throws NullPointerException if any parameter is {@code null}
   */
  public TaskContext(BlobService blobService,
                     TaskService taskService,
                     PayloadSerializer payloadSerializer,
                     SessionId sessionId,
                     Map<String, TaskInput> inputs,
                     Map<String, TaskOutput> outputs) {
    this.blobService = requireNonNull(blobService, "blobService cannot be null");
    this.taskService = requireNonNull(taskService, "taskService cannot be null");
    this.payloadSerializer = requireNonNull(payloadSerializer, "payloadSerializer cannot be null");
    this.sessionId = requireNonNull(sessionId, "sessionId cannot be null");
    this.inputs = requireNonNull(inputs, "inputs cannot be null");
    this.outputs = requireNonNull(outputs, "outputs cannot be null");
  }

  /**
   * Returns the identifier of the session in which this task is executing.
   *
   * @return the non-null identifier of the current session
   */
  public SessionId sessionId() {
    return sessionId;
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

    var allInputHandles = prepareInputHandles(taskDefinition);
    var allOutputHandles = prepareOutputHandles(taskDefinition);

    var deferredTaskInfo = getIdsFrom(allInputHandles).thenCombine(getIdsFrom(allOutputHandles), createPayload())
                                                      .thenCompose(payload -> createPayloadHandle(payload).thenCompose(submitTask(taskDefinition, payload)));
    trackTaskCompletion(deferredTaskInfo);

    return new TaskHandle(sessionId, taskDefinition.configuration(), deferredTaskInfo, allInputHandles, allOutputHandles);
  }

  /**
   * Waits for all pending asynchronous operations to complete.
   * <p>
   * This method blocks until all blob uploads and task submissions initiated through
   * this context have completed. It is called automatically by the worker infrastructure
   * after {@link TaskProcessor#processTask(TaskContext)} returns to ensure that all
   * operations finish before the task is marked as complete.
   * </p>
   *
   * <h4>What Operations Are Awaited</h4>
   * <p>
   * This method waits for completion of:
   * </p>
   * <ul>
   *   <li>Blob data uploads (from {@link #createBlob(InputBlobDefinition)})</li>
   *   <li>Task submissions (from {@link #submitTask(TaskDefinition)})</li>
   *   <li>Any deferred blob metadata creation</li>
   * </ul>
   *
   * <h4>Exception Handling</h4>
   * <p>
   * If any pending operation fails, this method throws an {@link ArmoniKException}
   * containing the failure cause. The worker infrastructure will report this as a
   * task error to the Agent.
   * </p>
   *
   * <h4>Thread Safety</h4>
   * <p>
   * This method is thread-safe and can be called multiple times (subsequent calls
   * return immediately if operations have already completed). However, it is designed
   * to be called once after task processing completes.
   * </p>
   *
   * @throws ArmoniKException if any pending operation fails or is interrupted
   * @see TaskProcessor#processTask(TaskContext)
   */
  public void awaitCompletion() {
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

  private static BiFunction<Map<String, BlobId>, Map<String, BlobId>, Payload> createPayload() {
    return (inputIds, outputIds) -> {
      logger.debug("Creating payload with input keys:{} and output keys:{}", inputIds.keySet(), outputIds.keySet());
      return Payload.from(inputIds, outputIds);
    };
  }

  private Map<String, BlobHandle> prepareInputHandles(TaskDefinition taskDefinition) {
    var createdInputHandles = blobService.createBlobs(taskDefinition.inputDefinitions());
    trackBlobCompletions(createdInputHandles.values());

    return merge(
      createdInputHandles,
      taskDefinition.inputHandles(),
      taskDefinition.taskInputHandles()
    );
  }

  private Map<String, BlobHandle> prepareOutputHandles(TaskDefinition taskDefinition) {
    var createdOutputBlobHandles = blobService.prepareBlobs(taskDefinition.outputDefinitions());
    trackBlobCompletions(createdOutputBlobHandles.values());

    return merge(
      createdOutputBlobHandles,
      taskDefinition.taskOutputHandles()
    );
  }

  private CompletionStage<BlobInfo> createPayloadHandle(Payload payload) {
    return blobService.createBlob(InputBlobDefinition.from("payload", payloadSerializer.serialize(payload))).deferredBlobInfo();
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

  /**
   * Merges multiple maps into a single map.
   * <p>
   * Entries from later maps in the argument list override entries with the same key
   * from earlier maps. The returned map is a new {@link HashMap} and modifications to
   * it do not affect the original maps.
   * </p>
   *
   * @param maps the maps to merge; must not be {@code null} and must not contain {@code null} maps
   * @param <K>  the key type
   * @param <V>  the value type
   * @return a new map containing all entries from the provided maps; never {@code null}
   */
  @SafeVarargs
  private static <K, V> Map<K, V> merge(Map<K, V>... maps) {
    var result = new HashMap<K, V>();
    for (var map : maps) {
      result.putAll(map);
    }
    return result;
  }
}
