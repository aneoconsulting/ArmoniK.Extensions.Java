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
package fr.aneo.armonik.client.model;

import com.google.gson.Gson;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest.TaskCreation;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksGrpc;
import fr.aneo.armonik.client.definition.BlobDefinition;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import fr.aneo.armonik.client.internal.grpc.mappers.TaskMapper;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.CreateResultsMetaDataResponse;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ResultRaw;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc.ResultsFutureStub;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc.newFutureStub;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksGrpc.TasksFutureStub;
import static fr.aneo.armonik.client.internal.concurrent.Futures.toCompletionStage;
import static fr.aneo.armonik.client.internal.grpc.mappers.BlobMapper.toResultMetaDataRequest;
import static fr.aneo.armonik.client.internal.grpc.mappers.TaskMapper.toSubmitTasksRequest;
import static fr.aneo.armonik.client.model.TaskConfiguration.defaultConfiguration;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
/**
 * Internal component responsible for submitting tasks to the ArmoniK cluster.
 * <p>
 * This class handles the complex process of task submission including blob allocation,
 * data upload, task creation, and output completion coordination. It serves as the
 * implementation backend for {@link SessionHandle#submitTask(TaskDefinition)} and
 * manages all gRPC interactions required for task submission.
 * <p>
 * TaskSubmitter is an internal implementation class and should not be used directly
 * by client applications. Use {@link SessionHandle} for task submission operations.
 *
 * @see SessionHandle#submitTask(TaskDefinition)
 * @see TaskHandle
 * @see BlobCompletionCoordinator
 */
final class TaskSubmitter {
  private static final Logger logger = LoggerFactory.getLogger(TaskSubmitter.class);

  private final SessionId sessionId;
  private final TaskConfiguration defaultTaskConfiguration;
  private final ManagedChannel channel;
  private final TasksFutureStub tasksStub;
  private final ResultsFutureStub resultsFutureStub;
  private final Gson gson = new Gson();
  private BlobCompletionCoordinator blobCompletionCoordinator;

  /**
   * Creates a new task submitter for the specified session context.
   * <p>
   * The task submitter will use the provided session identifier and default task
   * configuration for all submitted tasks. It will coordinate with the blob completion
   * listener to handle output processing events.
   *
   * @param sessionId the identifier of the session context for task submission
   * @param taskConfiguration the default configuration to apply to submitted tasks
   * @param outputTaskListener the listener to receive task output completion events
   * @param channel the gRPC channel for cluster communication
   * @throws NullPointerException if any parameter is null
   * @see BlobCompletionListener
   */
  TaskSubmitter(SessionId sessionId,
                       TaskConfiguration taskConfiguration,
                       BlobCompletionListener outputTaskListener,
                       ManagedChannel channel) {

    this.sessionId = sessionId;
    this.defaultTaskConfiguration = taskConfiguration != null ? taskConfiguration : defaultConfiguration();
    this.channel = channel;
    this.resultsFutureStub = newFutureStub(channel);
    this.tasksStub = TasksGrpc.newFutureStub(channel);

    if (outputTaskListener != null) {
      this.blobCompletionCoordinator = new BlobCompletionCoordinator(channel, outputTaskListener);
    }
  }

  /**
   * Submits a task for execution based on the provided task definition.
   * <p>
   * This method orchestrates the complete task submission process, including:
   * <ul>
   *   <li>Allocating blob handles for inputs and outputs</li>
   *   <li>Uploading input data to the cluster</li>
   *   <li>Creating the task with proper dependencies</li>
   *   <li>Registering output completion monitoring</li>
   * </ul>
   * The returned handle provides access to task metadata and associated blobs.
   *
   * @param taskDefinition the definition specifying task inputs, outputs, and configuration
   * @return a handle representing the submitted task
   * @throws NullPointerException if taskDefinition is null
   * @throws RuntimeException if task submission fails
   * @see TaskDefinition
   * @see TaskHandle
   */
  public TaskHandle submit(TaskDefinition taskDefinition) {
    requireNonNull(taskDefinition, "taskDefinition must not be null");

    var allocation = allocateBlobHandles(taskDefinition);
    uploadInputs(allocation, taskDefinition.inputDefinitions());
    var allInputHandles = Stream.concat(allocation.inputHandlesByName().entrySet().stream(), taskDefinition.inputHandles().entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    uploadPayload(allInputHandles, allocation);

    if (blobCompletionCoordinator != null) {
      blobCompletionCoordinator.enqueue(sessionId, allocation.outputHandlesByName().values().stream().toList());
    }

    var inputIdStages = getIdsFrom(allInputHandles.values());
    var outputIdStages = getIdsFrom(allocation.outputHandlesByName().values());
    var deferredTaskInfo = allocation.payloadHandle()
                                     .deferredBlobInfo()
                                     .thenApply(BlobInfo::id)
                                     .thenCombine(inputIdStages, AllInputs::new)
                                     .thenCombine(outputIdStages, toTaskCreation(taskDefinition))
                                     .thenApply(taskCreation -> toSubmitTasksRequest(sessionId, defaultTaskConfiguration, taskCreation))
                                     .thenCompose(request -> toCompletionStage(tasksStub.submitTasks(request)))
                                     .thenApply(response -> new TaskInfo(TaskId.from(response.getTaskInfos(0).getTaskId())));

    return new TaskHandle(
      sessionId,
      taskDefinition.configuration() != null ? taskDefinition.configuration() : defaultTaskConfiguration,
      deferredTaskInfo,
      allInputHandles,
      allocation.outputHandlesByName()
    );
  }

  /**
   * Returns a completion stage that completes when all currently tracked output operations finish.
   * <p>
   * This method provides access to the underlying blob completion coordinator's
   * wait functionality, allowing callers to synchronize on output processing completion.
   *
   * @return a completion stage that completes when all current output operations finish
   * @see BlobCompletionCoordinator#waitUntilFinished()
   */
  public CompletionStage<Void> waitUntilFinished() {
    return blobCompletionCoordinator.waitUntilFinished();
  }

  private static BiFunction<AllInputs, List<BlobId>, TaskCreation> toTaskCreation(TaskDefinition taskDefinition) {
    return (allInputs, outputIds) -> TaskMapper.toTaskCreation(allInputs.inputIds, outputIds, allInputs.payloadId, taskDefinition.configuration());
  }

  private BlobHandlesAllocation allocateBlobHandles(TaskDefinition taskDefinition) {
    logger.atDebug()
          .addKeyValue("operation", "allocateBlobHandles")
          .addKeyValue("sessionId", sessionId.asString())
          .log("Allocating Blob Handles");

    int inputDefinitionCount = taskDefinition.inputDefinitions().size();
    int outputCount = taskDefinition.outputs().size();
    int totalHandleCount = inputDefinitionCount + outputCount + 1;

    var resultRaws = Futures.toCompletionStage(resultsFutureStub.createResultsMetaData(toResultMetaDataRequest(sessionId, totalHandleCount)))
                            .thenApply(CreateResultsMetaDataResponse::getResultsList);
    var blobHandles = IntStream.range(0, totalHandleCount)
                               .mapToObj(toBlobHandle(sessionId, resultRaws))
                               .toList();
    var payloadHandle = blobHandles.get(0);
    var outputHandles = blobHandles.subList(1, 1 + outputCount);
    var inputHandles = blobHandles.subList(1 + outputCount, totalHandleCount);
    var outputHandleByName = zip(taskDefinition.outputs(), outputHandles);
    var inputHandleByName = zip(taskDefinition.inputDefinitions().keySet(), inputHandles);

    return new BlobHandlesAllocation(payloadHandle, inputHandleByName, outputHandleByName);
  }

  private void uploadPayload(Map<String, BlobHandle> allInputHandlesByName, BlobHandlesAllocation allocation) {
    var inputIdByName = allInputHandlesByName.entrySet().stream().collect(ids());
    var outputIdByName = allocation.outputHandlesByName().entrySet().stream().collect(ids());

    Futures.allOf(inputIdByName)
           .thenCombine(Futures.allOf(outputIdByName), this::serializeToJson)
           .thenCompose(payloadDefinition -> allocation.payloadHandle().uploadData(payloadDefinition));
  }

  private void uploadInputs(BlobHandlesAllocation allocation, Map<String, BlobDefinition> inputDefinitions) {
    allocation.inputHandlesByName()
              .entrySet()
              .stream()
              .collect(toMap(
                Map.Entry::getValue,
                entry -> inputDefinitions.get(entry.getKey())))
              .forEach(BlobHandle::uploadData);
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

  private record AllInputs(BlobId payloadId, List<BlobId> inputIds) {
  }

  public record BlobHandlesAllocation(
    BlobHandle payloadHandle,
    Map<String, BlobHandle> inputHandlesByName,
    Map<String, BlobHandle> outputHandlesByName
  ) {
  }

  private IntFunction<BlobHandle> toBlobHandle(SessionId sessionId, CompletionStage<List<ResultRaw>> resultRaws) {
    return index -> new BlobHandle(
      sessionId,
      resultRaws.thenApply(resultRaw -> new BlobInfo(BlobId.from(resultRaw.get(index).getResultId()))),
      channel
    );
  }

  private static <K, V> Map<K, V> zip(Collection<K> keys, Collection<V> values) {
    if (keys.size() != values.size()) {
      throw new IllegalArgumentException("Collections must have the same size");
    }
    Iterator<K> keyIt = keys.iterator();
    Iterator<V> valIt = values.iterator();

    Map<K, V> map = new LinkedHashMap<>(keys.size());
    while (keyIt.hasNext() && valIt.hasNext()) {
      map.put(keyIt.next(), valIt.next());
    }
    return map;
  }

  public BlobDefinition serializeToJson(Map<String, BlobId> inputIds, Map<String, BlobId> outputIds) {
    var in = inputIds.entrySet()
                     .stream()
                     .collect(toMap(Map.Entry::getKey, e -> e.getValue().asString()));
    var out = outputIds.entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, e -> e.getValue().asString()));

    var payload = Map.of("inputs", in, "outputs", out);
    byte[] bytes = gson.toJson(payload).getBytes(UTF_8);

    return BlobDefinition.from(bytes);
  }
}
