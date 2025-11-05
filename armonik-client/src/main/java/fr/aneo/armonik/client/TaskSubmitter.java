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
package fr.aneo.armonik.client;

import com.google.gson.Gson;
import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksGrpc;
import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.BlobDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import fr.aneo.armonik.client.internal.grpc.mappers.TaskMapper;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.CreateResultsMetaDataResponse;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ResultRaw;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksResponse;
import static fr.aneo.armonik.client.TaskConfiguration.defaultConfiguration;
import static fr.aneo.armonik.client.WorkerLibrary.LIBRARY_BLOB_ID;
import static fr.aneo.armonik.client.internal.grpc.mappers.BlobMapper.toResultMetaDataRequest;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Internal component responsible for orchestrating task submission to the ArmoniK cluster.
 * <p>
 * This class handles the complete task submission pipeline, including blob allocation, data upload,
 * task creation, and output completion coordination. It serves as the implementation backend for
 * {@link SessionHandle#submitTask(TaskDefinition)} and manages all gRPC interactions required
 * for task submission.
 * <p>
 * The task submission process follows these steps:
 * <ol>
 *   <li><strong>Blob Allocation:</strong> Allocates blob handles for inputs, outputs, and payload</li>
 *   <li><strong>Input Upload:</strong> Uploads input data from {@link BlobDefinition}s to the cluster</li>
 *   <li><strong>Payload Creation:</strong> Serializes task metadata (input/output mappings) as JSON payload</li>
 *   <li><strong>Task Submission:</strong> Submits the task with dependencies to the ArmoniK cluster</li>
 *   <li><strong>Output Monitoring:</strong> Registers output blobs for completion monitoring (if configured)</li>
 * </ol>
 * <p>
 * TaskSubmitter is an internal implementation class and should not be used directly
 * by client applications. Use {@link SessionHandle} for task submission operations.
 * <p>
 * This class is thread-safe and supports concurrent task submissions within the same session.
 *
 * @see SessionHandle#submitTask(TaskDefinition)
 * @see TaskHandle
 * @see BlobCompletionCoordinator
 */
final class TaskSubmitter {
  private static final Logger logger = LoggerFactory.getLogger(TaskSubmitter.class);

  private final SessionId sessionId;
  private final TaskConfiguration defaultTaskConfiguration;
  private final ChannelPool channelPool;
  private final Gson gson = new Gson();
  private BlobCompletionCoordinator blobCompletionCoordinator;

  /**
   * Creates a new task submitter for the specified session context.
   * <p>
   * The task submitter will use the session's configuration for default task settings and
   * will optionally set up blob completion coordination if an output listener is configured
   * in the session definition.
   *
   * @param sessionId         the identifier of the session context for task submission
   * @param sessionDefinition the session definition containing task configuration and output listener
   * @param channelPool       the gRPC channel pool for cluster communication
   * @throws NullPointerException if any parameter is null
   * @see SessionDefinition
   * @see BlobCompletionCoordinator
   */
  TaskSubmitter(SessionId sessionId,
                SessionDefinition sessionDefinition,
                ChannelPool channelPool) {

    this.sessionId = sessionId;
    this.defaultTaskConfiguration = sessionDefinition.taskConfiguration() != null ? sessionDefinition.taskConfiguration() : defaultConfiguration();
    this.channelPool = channelPool;

    if (sessionDefinition.outputListener() != null) {
      this.blobCompletionCoordinator = new BlobCompletionCoordinator(sessionId, channelPool, sessionDefinition);
    }
  }

  /**
   * Submits a task for execution based on the provided task definition.
   * <p>
   * This method orchestrates the complete task submission process:
   * <ol>
   *   <li><strong>Blob Allocation:</strong> Allocates blob handles for all inputs, outputs, and the task payload</li>
   *   <li><strong>Data Upload:</strong> Uploads input data content to the allocated blob handles</li>
   *   <li><strong>Payload Upload:</strong> Creates and uploads a JSON payload containing input/output mappings</li>
   *   <li><strong>Task Creation:</strong> Submits the task to ArmoniK with proper dependencies</li>
   *   <li><strong>Output Monitoring:</strong> Registers output blobs with the completion coordinator</li>
   * </ol>
   * The returned {@link TaskHandle} provides immediate access to task metadata and blob handles,
   * while the actual submission continues asynchronously in the background.
   * <p>
   * <strong>Task Configuration Priority:</strong> If the task definition specifies a configuration,
   * it takes precedence over the session's default configuration. Otherwise, the session default applies.
   *
   * @param taskDefinition the definition specifying task inputs, outputs, and optional configuration
   * @return a handle representing the submitted task with access to inputs, outputs, and metadata
   * @throws NullPointerException if taskDefinition is null
   * @throws RuntimeException     if task submission fails due to cluster communication issues,
   *                              blob allocation failures, or data upload errors
   * @see TaskDefinition
   * @see TaskHandle
   * @see BlobHandle
   */
  TaskHandle submit(TaskDefinition taskDefinition) {
    requireNonNull(taskDefinition, "taskDefinition must not be null");

    var allocation = allocateBlobHandles(taskDefinition);
    uploadInputs(allocation, taskDefinition.inputDefinitions());
    var allInputHandlesByName = Stream.concat(
                                        allocation.inputHandlesByName().entrySet().stream(),
                                        taskDefinition.inputHandles().entrySet().stream())
                                      .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    uploadPayload(allInputHandlesByName, allocation);

    if (blobCompletionCoordinator != null) {
      blobCompletionCoordinator.enqueue(allocation.outputHandlesByName().values().stream().toList());
    }

    var inputIdsStage = getIdsFrom(allInputHandlesByName.values());
    var outputIdsStage = getIdsFrom(allocation.outputHandlesByName().values());
    var payloadIdStage = allocation.payloadHandle.deferredBlobInfo().thenApply(BlobInfo::id);

    var deferredTaskInfo = payloadIdStage.thenApply(id -> new TaskSubmissionData(sessionId, id))
                                         .thenCombine(inputIdsStage, TaskSubmissionData::withInputIds)
                                         .thenCombine(outputIdsStage, TaskSubmissionData::withOutputIds)
                                         .thenCombine(taskDefinition.workerLibrary().asDeferredTaskOptions(), TaskSubmissionData::withWorkerLibraryOptions)
                                         .thenApply(submission -> submission.withSessionTaskConfig(defaultTaskConfiguration))
                                         .thenApply(submission -> submission.withTaskConfig(taskDefinition.configuration()))
                                         .thenApply(TaskSubmissionData::toSubmitTasksRequest)
                                         .thenCompose(this::submitTask)
                                         .thenApply(response -> new TaskInfo(TaskId.from(response.getTaskInfos(0).getTaskId())))
                                         .whenComplete((info, ex) -> {
                                           if (ex != null)
                                             logger.error("Task submission failed. Session id:{}", sessionId.asString(), ex);
                                         });

    return new TaskHandle(
      sessionId,
      taskDefinition.configuration() != null ? taskDefinition.configuration() : defaultTaskConfiguration,
      deferredTaskInfo,
      allInputHandlesByName,
      allocation.outputHandlesByName()
    );
  }

  /**
   * Returns a completion stage that completes when all currently tracked output operations finish.
   * <p>
   * This method provides access to the underlying blob completion coordinator's synchronization
   * mechanism, allowing callers to wait for all pending output processing to complete. This is
   * useful for ensuring that all task outputs have been processed before proceeding with
   * dependent operations.
   * <p>
   * If no blob completion coordinator is configured (i.e., any output listener in the session),
   * this method may return a null completion stage.
   *
   * @return a completion stage that completes when all current output operations finish,
   * or null if no output coordination is configured
   * @see BlobCompletionCoordinator#waitUntilIdle()
   * @see SessionHandle#awaitOutputsProcessed()
   */
  CompletionStage<Void> waitUntilFinished() {
    return blobCompletionCoordinator.waitUntilIdle();
  }

  /**
   * Allocates blob handles for all inputs, outputs, and the task payload.
   * The method sends blob metadata (name and deletion policy) to the cluster, which responds
   * with assigned blob IDs. These IDs are wrapped in {@link BlobHandle} instances that
   * provide data upload and download capabilities.
   *
   * @param taskDefinition the task definition containing input and output specifications
   * @return an allocation record containing the payload handle and maps of input/output handles
   * keyed by their parameter names
   * @see BlobHandlesAllocation
   * @see BlobDefinition
   */
  private BlobHandlesAllocation allocateBlobHandles(TaskDefinition taskDefinition) {
    var inputDefinitions = new ArrayList<>(taskDefinition.inputDefinitions().entrySet());
    var outputDefinitions = new ArrayList<>(taskDefinition.outputDefinitions().entrySet());
    var payload = InputBlobDefinition.from(new byte[0]).withName("payload");

    var allDefinitions = new ArrayList<BlobDefinition>(inputDefinitions.size() + outputDefinitions.size() + 1);
    allDefinitions.add(payload);
    allDefinitions.addAll(inputDefinitions.stream().map(Map.Entry::getValue).toList());
    allDefinitions.addAll(outputDefinitions.stream().map(Map.Entry::getValue).toList());

    var resultRaws = channelPool.executeAsync(createMetadata(allDefinitions))
                                .thenApply(CreateResultsMetaDataResponse::getResultsList);

    var blobHandles = IntStream.range(0, allDefinitions.size())
                               .mapToObj(toBlobHandle(sessionId, resultRaws))
                               .toList();
    var payloadHandle = blobHandles.get(0);
    var inputHandles = blobHandles.subList(1, 1 + inputDefinitions.size());
    var outputHandles = blobHandles.subList(1 + inputDefinitions.size(), allDefinitions.size());


    var inputHandleByName = zip(inputDefinitions, inputHandles);
    var outputHandleByName = zip(outputDefinitions, outputHandles);

    return new BlobHandlesAllocation(payloadHandle, inputHandleByName, outputHandleByName);
  }

  private Function<ManagedChannel, CompletionStage<CreateResultsMetaDataResponse>> createMetadata(ArrayList<BlobDefinition> allDefinitions) {
    return channel -> Futures.toCompletionStage(ResultsGrpc.newFutureStub(channel).createResultsMetaData(toResultMetaDataRequest(sessionId, allDefinitions)));
  }

  private CompletionStage<SubmitTasksResponse> submitTask(SubmitTasksRequest request) {
    return channelPool.executeAsync(channel -> Futures.toCompletionStage(TasksGrpc.newFutureStub(channel).submitTasks(request)));
  }

  private void uploadPayload(Map<String, BlobHandle> allInputHandlesByName, BlobHandlesAllocation allocation) {
    var inputIdByName = allInputHandlesByName.entrySet().stream().collect(ids());
    var outputIdByName = allocation.outputHandlesByName().entrySet().stream().collect(ids());

    Futures.allOf(inputIdByName)
           .thenCombine(Futures.allOf(outputIdByName), this::serializeToJson)
           .thenCompose(payloadDefinition -> allocation.payloadHandle().uploadData(payloadDefinition.data()));
  }

  private void uploadInputs(BlobHandlesAllocation allocation, Map<String, InputBlobDefinition> inputDefinitions) {
    allocation.inputHandlesByName()
              .entrySet()
              .stream()
              .collect(toMap(
                Map.Entry::getValue,
                entry -> inputDefinitions.get(entry.getKey()).data()))
              .forEach(BlobHandle::uploadData);
  }

  private InputBlobDefinition serializeToJson(Map<String, BlobId> inputIds, Map<String, BlobId> outputIds) {
    var in = inputIds.entrySet()
                     .stream()
                     .collect(toMap(Map.Entry::getKey, e -> e.getValue().asString()));
    var out = outputIds.entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, e -> e.getValue().asString()));

    var payload = Map.of("inputs", in, "outputs", out);
    byte[] bytes = gson.toJson(payload).getBytes(UTF_8);

    return InputBlobDefinition.from(bytes).withName("payload");
  }

  private static Collector<Map.Entry<String, BlobHandle>, ?, Map<String, CompletionStage<BlobId>>> ids() {
    return toMap(
      Map.Entry::getKey,
      entry -> entry.getValue().deferredBlobInfo().thenApply(BlobInfo::id)
    );
  }

  private static CompletionStage<List<BlobId>> getIdsFrom(Collection<BlobHandle> blobHandles) {
    return Futures.allOf(blobHandles.stream().map(BlobHandle::deferredBlobInfo).toList())
                  .thenApply(blobInfos -> blobInfos.stream().map(BlobInfo::id).toList());
  }

  private IntFunction<BlobHandle> toBlobHandle(SessionId sessionId, CompletionStage<List<ResultRaw>> resultRaws) {
    return index -> new BlobHandle(
      sessionId,
      resultRaws.thenApply(raws -> toBlobInfo(sessionId, raws.get(index))),
      channelPool
    );
  }

  private static BlobInfo toBlobInfo(SessionId sessionId, ResultRaw resultRaw) {
    var resultId = BlobId.from(resultRaw.getResultId());
    var name = resultRaw.getName();
    var manualDeletion = resultRaw.getManualDeletion();
    var createdBy = resultRaw.getCreatedBy();
    var createdAt = resultRaw.getCreatedAt();

    var createdByTask = createdBy.isBlank() ? null : TaskId.from(createdBy);
    var createdAtInstant = Instant.ofEpochSecond(createdAt.getSeconds(), createdAt.getNanos());

    return new BlobInfo(resultId, sessionId, name, manualDeletion, createdByTask, createdAtInstant);
  }

  private static <K, V, U> Map<K, U> zip(List<Map.Entry<K, V>> keys, List<U> values) {
    if (keys.size() != values.size()) {
      throw new IllegalArgumentException("Collections must have the same size");
    }

    return IntStream.range(0, keys.size())
                    .boxed()
                    .collect(toMap(
                      i -> keys.get(i).getKey(),
                      values::get)
                    );
  }

  /**
   * Internal record representing the allocated blob handles for a task submission.
   * <p>
   * This record groups all blob handles allocated during task submission, providing
   * organized access to payload, input, and output blob handles by their logical names.
   *
   * @param payloadHandle       the blob handle for the task's JSON payload containing input/output mappings
   * @param inputHandlesByName  map of input blob handles keyed by their logical input names
   * @param outputHandlesByName map of output blob handles keyed by their logical output names
   * @see #allocateBlobHandles(TaskDefinition)
   */
  private record BlobHandlesAllocation(BlobHandle payloadHandle,
                                       Map<String, BlobHandle> inputHandlesByName,
                                       Map<String, BlobHandle> outputHandlesByName) {
  }

  /**
   * Internal record representing the staged assembly of task submission data.
   * <p>
   * This record models the progressive accumulation of data required for task submission,
   * supporting a functional, immutable assembly pipeline. Each stage of the submission
   * process adds data to the record through immutable transformations, eventually producing
   * a complete {@link SubmitTasksRequest} for the Tasks service.
   * <p>
   * <strong>Assembly Pipeline Stages:</strong>
   * <ol>
   *   <li><strong>Initial:</strong> Session ID and payload ID</li>
   *   <li><strong>+ Input IDs:</strong> Add resolved input blob IDs</li>
   *   <li><strong>+ Output IDs:</strong> Add resolved output blob IDs</li>
   *   <li><strong>+ Worker Library Options:</strong> Add SDK convention options from {@link WorkerLibrary}</li>
   *   <li><strong>+ Session Config:</strong> Add default task configuration from session</li>
   *   <li><strong>+ Task Config:</strong> Add task-specific configuration (if present)</li>
   *   <li><strong>Final:</strong> Convert to gRPC {@link SubmitTasksRequest}</li>
   * </ol>
   * <p>
   * <strong>Immutability Pattern:</strong>
   * <p>
   * Each {@code with*()} method creates a new record instance with the additional data,
   * preserving the original record. This functional approach ensures thread safety and
   * enables clean composition in the asynchronous submission pipeline.
   * <p>
   *
   * @param sessionId            the session ID for task ownership
   * @param payloadId            the blob ID of the JSON name-to-ID mapping payload
   * @param inputBlobIds         list of input blob IDs (data dependencies)
   * @param outputBlobIds        list of output blob IDs (expected results)
   * @param sessionTaskConfig    default task configuration from session definition
   * @param taskConfig           task-specific configuration (may be null if using session defaults)
   * @param workerLibraryOptions worker library options to merge into configuration
   * @see #submit(TaskDefinition)
   */
  private record TaskSubmissionData(
    SessionId sessionId,
    BlobId payloadId,
    List<BlobId> inputBlobIds,
    List<BlobId> outputBlobIds,
    TaskConfiguration sessionTaskConfig,
    TaskConfiguration taskConfig,
    Map<String, String> workerLibraryOptions
  ) {

    TaskSubmissionData(SessionId sessionId, BlobId payloadId) {
      this(sessionId, payloadId, List.of(), List.of(), null, null, Map.of());
    }

    TaskSubmissionData withInputIds(List<BlobId> inputBlobIds) {
      return new TaskSubmissionData(sessionId, payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, workerLibraryOptions);
    }

    TaskSubmissionData withOutputIds(List<BlobId> outputBlobIds) {
      return new TaskSubmissionData(sessionId, payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, workerLibraryOptions);
    }

    TaskSubmissionData withSessionTaskConfig(TaskConfiguration sessionTaskConfig) {
      return new TaskSubmissionData(sessionId, payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, workerLibraryOptions);
    }

    TaskSubmissionData withTaskConfig(TaskConfiguration taskConfig) {
      return new TaskSubmissionData(sessionId, payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, workerLibraryOptions);
    }

    TaskSubmissionData withWorkerLibraryOptions(Map<String, String> workerLibraryOptions) {
      var effectiveOptions = workerLibraryOptions != null ? workerLibraryOptions : Map.<String, String>of();
      return new TaskSubmissionData(sessionId, payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, effectiveOptions);
    }

    SubmitTasksRequest toSubmitTasksRequest() {
      var effectiveTaskConfig = taskConfig != null ?
        taskConfig.withOptions(workerLibraryOptions) :
        sessionTaskConfig.withOptions(workerLibraryOptions);

      var dataDependencies = new ArrayList<>(inputBlobIds);
      if (!workerLibraryOptions.isEmpty()) {
        dataDependencies.add(BlobId.from(workerLibraryOptions.get(LIBRARY_BLOB_ID)));
      }

      var taskCreation = TaskMapper.toTaskCreation(dataDependencies, outputBlobIds, payloadId, effectiveTaskConfig);
      return TaskMapper.toSubmitTasksRequest(sessionId, sessionTaskConfig, taskCreation);
    }
  }
}
