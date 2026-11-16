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
import fr.aneo.armonik.client.definition.TaskDefinition;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import fr.aneo.armonik.client.definition.blob.BlobData;
import fr.aneo.armonik.client.definition.blob.BlobDefinition;
import fr.aneo.armonik.client.definition.blob.InMemoryBlobData;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.client.exception.ArmoniKException;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import fr.aneo.armonik.client.internal.concurrent.Schedulers;
import fr.aneo.armonik.client.internal.grpc.mappers.TaskMapper;
import fr.aneo.armonik.client.internal.retry.RetryableStreamOperation;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.CreateResultsMetaDataResponse;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.CreateResultsRequest;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.CreateResultsResponse;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.ResultRaw;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest.TaskCreation;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksResponse;
import static fr.aneo.armonik.client.TaskConfiguration.defaultConfiguration;
import static fr.aneo.armonik.client.WorkerLibrary.LIBRARY_BLOB_ID;
import static fr.aneo.armonik.client.internal.concurrent.ExecutorProvider.defaultExecutor;
import static fr.aneo.armonik.client.internal.grpc.mappers.BlobMapper.toResultMetaDataRequest;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toMap;

/**
 * Internal component responsible for orchestrating task submission to the ArmoniK cluster.
 * <p>
 * This class handles the complete task submission pipeline, including blob allocation, data upload,
 * task creation, and output completion coordination. It serves as the implementation backend for
 * {@link SessionHandle#submitTask(TaskDefinition)} and {@link SessionHandle#submitTasks(List)},
 * and manages all gRPC interactions required
 * for task submission.
 * <p>
 * The task submission process follows these steps:
 * <ol>
 *   <li><strong>Blob Allocation:</strong> Allocates blob handles for inputs, outputs, and payload</li>
 *   <li><strong>Input Creation:</strong> Creates the input blobs; small in-memory inputs go with their
 *       data in batched {@code CreateResults} calls, the others are uploaded as streams</li>
 *   <li><strong>Payload Creation:</strong> Serializes the input/output mappings as a JSON payload and creates
 *       it the same way, once the ids it names are known</li>
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

  /**
   * Maximum number of blobs in one {@code CreateResultsMetaData} or {@code CreateResults} call: {@value}.
   */
  static final int MAX_BLOBS_PER_REQUEST = 1000;

  /**
   * Maximum number of tasks in one {@code SubmitTasks} call: {@value}.
   */
  static final int MAX_TASKS_PER_REQUEST = 1000;
  private static final String PAYLOAD_NAME = "payload";

  private final SessionId sessionId;
  private final TaskConfiguration defaultTaskConfiguration;
  private final ChannelPool channelPool;
  private final TaskSubmissionBatching batching;
  private final ScheduledExecutorService scheduler;
  private final Executor flushExecutor;
  private final ResultsServiceConfiguration resultsServiceConfiguration;
  private final Gson gson = new Gson();
  /**
   * Listener used when the session has none, so that outputs are watched without null checks.
   * Their data is then not downloaded.
   */
  private static final BlobCompletionListener NO_LISTENER = new BlobCompletionListener() {
    @Override
    public void onSuccess(Blob blob) {
    }

    @Override
    public void onError(BlobError blobError) {
    }
  };

  private final BlobCompletionListener outputListener;
  private final BlobCompletionCoordinator blobCompletionCoordinator;

  private final Object lock = new Object();
  private final List<PendingTask> buffer = new ArrayList<>();
  private ScheduledFuture<?> timer;

  /**
   * Creates a new task submitter for the specified session context.
   * <p>
   * This constructor is used internally by {@link SessionHandle} when a session is opened
   * or retrieved via {@link ArmoniKClient}. It configures the default task settings for the
   * session and initializes output blob completion coordination using the default batching
   * strategy. Without a listener, outputs are still watched, so that {@link #waitUntilFinished()}
   * waits for them, but their data is not downloaded.
   * <p>
   * When {@code taskConfiguration} is {@code null}, the global
   * {@link TaskConfiguration#defaultConfiguration() default configuration} is applied.
   *
   * @param sessionId         the identifier of the session context for task submission
   * @param taskConfiguration the default task configuration to apply for submissions in this session;
   *                          may be {@code null} to use the global default
   * @param outputListener    optional listener for output blob completion events;
   *                          may be {@code null}
   * @param channelPool       the gRPC channel pool for cluster communication
   * @throws NullPointerException if {@code sessionId} or {@code channelPool} is {@code null}
   * @see SessionHandle
   * @see TaskConfiguration
   * @see BlobCompletionListener
   * @see BlobCompletionCoordinator
   */
  TaskSubmitter(SessionId sessionId,
                TaskConfiguration taskConfiguration,
                BlobCompletionListener outputListener,
                ChannelPool channelPool) {
    this(sessionId, taskConfiguration, outputListener, channelPool, TaskSubmissionBatching.DEFAULT, Schedulers.shared());
  }

  /**
   * Creates a new task submitter with explicit submission batching settings and scheduler.
   *
   * @param sessionId         the identifier of the session context for task submission
   * @param taskConfiguration the default task configuration of the session; may be {@code null}
   * @param outputListener    optional listener for output blob completion events;
   *                          may be {@code null}
   * @param channelPool       the gRPC channel pool for cluster communication
   * @param batching          how submitted tasks are grouped into batched calls
   * @param scheduler         the scheduler running the batching window timer
   * @throws NullPointerException if {@code sessionId}, {@code channelPool}, {@code batching} or
   *                              {@code scheduler} is {@code null}
   */
  TaskSubmitter(SessionId sessionId,
                TaskConfiguration taskConfiguration,
                BlobCompletionListener outputListener,
                ChannelPool channelPool,
                TaskSubmissionBatching batching,
                ScheduledExecutorService scheduler) {
    this(sessionId, taskConfiguration, outputListener, channelPool, batching, scheduler, defaultExecutor());
  }

  TaskSubmitter(SessionId sessionId,
                TaskConfiguration taskConfiguration,
                BlobCompletionListener outputListener,
                ChannelPool channelPool,
                TaskSubmissionBatching batching,
                ScheduledExecutorService scheduler,
                Executor flushExecutor) {
    this(sessionId, taskConfiguration, outputListener, channelPool, batching, scheduler, flushExecutor, new ResultsServiceConfiguration(channelPool));
  }

  /**
   * Creates a new task submitter with explicit batching settings, scheduler and flush executor.
   *
   * @param sessionId         the identifier of the session context for task submission
   * @param taskConfiguration the default task configuration of the session; may be {@code null}
   * @param outputListener    optional listener for output blob completion events;
   *                          may be {@code null}
   * @param channelPool       the gRPC channel pool for cluster communication
   * @param batching          how submitted tasks are grouped into batched calls
   * @param scheduler         the scheduler running the batching window timer
   * @param flushExecutor     the executor sending the buffer when the batching window elapses
   * @throws NullPointerException if any parameter other than {@code taskConfiguration} and
   *                              {@code outputListener} is {@code null}
   */
  TaskSubmitter(SessionId sessionId,
                TaskConfiguration taskConfiguration,
                BlobCompletionListener outputListener,
                ChannelPool channelPool,
                TaskSubmissionBatching batching,
                ScheduledExecutorService scheduler,
                Executor flushExecutor,
                ResultsServiceConfiguration resultsServiceConfiguration) {

    this.sessionId = requireNonNull(sessionId);
    this.resultsServiceConfiguration = requireNonNull(resultsServiceConfiguration, "resultsServiceConfiguration must not be null");
    this.defaultTaskConfiguration = requireNonNullElse(taskConfiguration, defaultConfiguration());
    this.channelPool = requireNonNull(channelPool);
    this.batching = requireNonNull(batching, "batching must not be null");
    this.scheduler = requireNonNull(scheduler, "scheduler must not be null");
    this.flushExecutor = requireNonNull(flushExecutor, "flushExecutor must not be null");
    this.outputListener = requireNonNullElse(outputListener, NO_LISTENER);
    this.blobCompletionCoordinator = new BlobCompletionCoordinator(sessionId, channelPool, this.outputListener, outputListener != null);
  }

  /**
   * Submits a task for execution based on the provided task definition.
   * <p>
   * This method orchestrates the complete task submission process:
   * <ol>
   *   <li><strong>Blob Allocation:</strong> Allocates blob handles for all inputs, outputs, and the task payload</li>
   *   <li><strong>Input Creation:</strong> Creates the inputs, small in-memory ones with their data in
   *       batched {@code CreateResults} calls, the others through an upload stream</li>
   *   <li><strong>Payload Creation:</strong> Creates the JSON payload containing input/output mappings, the same way</li>
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

    var pendingTask = prepare(taskDefinition);
    enqueue(List.of(pendingTask));
    return pendingTask.taskHandle();
  }

  /**
   * Submits several tasks and sends them without waiting for the batching window.
   * <p>
   * The tasks are sent as one batch, together with any task already buffered by
   * {@link #submit(TaskDefinition)}, whatever the configured batch size. The blob metadata is created
   * with one {@code CreateResultsMetaData} call per {@value #MAX_BLOBS_PER_REQUEST} blobs, and the
   * tasks are submitted with one {@code SubmitTasks} call per {@value #MAX_TASKS_PER_REQUEST} tasks.
   * When the cluster reports a positive {@code DataChunkMaxSize}, small in-memory inputs and payloads are
   * created with their data in batched {@code CreateResults} calls; other inputs, or all of them when it
   * reports none, are uploaded with one stream per blob.
   * <p>
   * Tasks submitted in the same {@code SubmitTasks} call succeed or fail together.
   *
   * @param taskDefinitions the definitions of the tasks to submit
   * @return one handle per definition, in the order of {@code taskDefinitions}
   * @throws NullPointerException if {@code taskDefinitions} is null or contains null
   */
  List<TaskHandle> submitAll(List<TaskDefinition> taskDefinitions) {
    requireNonNull(taskDefinitions, "taskDefinitions must not be null");
    taskDefinitions.forEach(definition -> requireNonNull(definition, "taskDefinitions must not contain null"));
    if (taskDefinitions.isEmpty()) return List.of();

    var pendingTasks = taskDefinitions.stream().map(this::prepare).toList();
    var batch = new ArrayList<PendingTask>();
    synchronized (lock) {
      batch.addAll(drain(buffer.size()));
    }
    batch.addAll(pendingTasks);
    send(batch);
    return pendingTasks.stream().map(PendingTask::taskHandle).toList();
  }

  /**
   * Sends all buffered tasks now.
   *
   * @return a stage that completes when the cluster has acknowledged or rejected every task that
   * was buffered at the time of the call; it never completes exceptionally
   */
  CompletionStage<Void> flush() {
    List<PendingTask> batch;
    synchronized (lock) {
      batch = drain(buffer.size());
    }
    return send(batch);
  }

  /**
   * Returns a completion stage that completes when all currently tracked output operations finish.
   * <p>
   * This method provides access to the underlying blob completion coordinator's synchronization
   * mechanism, allowing callers to wait for all pending output processing to complete. This is
   * useful for ensuring that all task outputs have been processed before proceeding with
   * dependent operations.
   *
   * @return a completion stage that completes when all current output operations finish
   * @see BlobCompletionCoordinator#waitUntilIdle()
   * @see SessionHandle#awaitOutputsProcessed()
   */
  CompletionStage<Void> waitUntilFinished() {
    return blobCompletionCoordinator.waitUntilIdle();
  }

  /**
   * Builds the handles of a task and chains its uploads and its task creation on blob infos that
   * are completed later, when the task is sent with {@link #send(List)}.
   */
  private PendingTask prepare(TaskDefinition taskDefinition) {
    var payloadDefinition = InputBlobDefinition.from(new byte[0]).withName(PAYLOAD_NAME);
    var inputDefinitions = new ArrayList<>(taskDefinition.inputDefinitions().entrySet());
    var outputDefinitions = new ArrayList<>(taskDefinition.outputDefinitions().entrySet());

    var blobDefinitions = new ArrayList<BlobDefinition>(1 + inputDefinitions.size() + outputDefinitions.size());
    blobDefinitions.add(payloadDefinition);
    inputDefinitions.forEach(entry -> blobDefinitions.add(entry.getValue()));
    outputDefinitions.forEach(entry -> blobDefinitions.add(entry.getValue()));

    var blobInfos = blobDefinitions.stream().map(definition -> new CompletableFuture<BlobInfo>()).toList();
    var blobHandles = blobInfos.stream().map(blobInfo -> new BlobHandle(sessionId, blobInfo, channelPool)).toList();
    var outputStart = 1 + inputDefinitions.size();
    var allocation = new BlobHandlesAllocation(
      blobHandles.get(0),
      zip(inputDefinitions, blobHandles.subList(1, outputStart)),
      zip(outputDefinitions, blobHandles.subList(outputStart, blobHandles.size())));

    var allInputHandlesByName = Stream.concat(
                                        allocation.inputHandlesByName().entrySet().stream(),
                                        taskDefinition.inputHandles().entrySet().stream())
                                      .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    var payloadUpload = new CompletableFuture<Void>();
    // An input is ready once its data is on the cluster. Until send() picks how it is created, the
    // handle must not report it ready: another task, even in another session, may already wait on it.
    var inputReadiness = new ArrayList<CompletableFuture<Void>>();
    allocation.inputHandlesByName().values().forEach(handle -> {
      var ready = new CompletableFuture<Void>();
      handle.readyWhen(ready);
      inputReadiness.add(ready);
    });

    var inputIdsStage = getIdsFrom(allInputHandlesByName.values());
    var outputIdsStage = getIdsFrom(allocation.outputHandlesByName().values());
    var payloadIdStage = allocation.payloadHandle().deferredBlobInfo().thenApply(BlobInfo::id);
    var deferredTaskCreation = payloadIdStage.thenApply(TaskSubmissionData::new)
                                             .thenCombine(inputIdsStage, TaskSubmissionData::withInputIds)
                                             .thenCombine(outputIdsStage, TaskSubmissionData::withOutputIds)
                                             .thenCombine(taskDefinition.workerLibrary().asDeferredTaskOptions(), TaskSubmissionData::withWorkerLibraryOptions)
                                             .thenApply(submission -> submission.withSessionTaskConfig(defaultTaskConfiguration))
                                             .thenApply(submission -> submission.withTaskConfig(taskDefinition.configuration()))
                                             .thenApply(TaskSubmissionData::toTaskCreation);

    var taskInfo = new CompletableFuture<TaskInfo>();
    var taskHandle = new TaskHandle(
      sessionId,
      taskDefinition.configuration() != null ? taskDefinition.configuration() : defaultTaskConfiguration,
      taskInfo,
      allInputHandlesByName,
      allocation.outputHandlesByName()
    );
    allocation.outputHandlesByName().values().forEach(output -> output.readyWhen(taskInfo));
    watchOutputsOnceSubmitted(taskInfo, allocation.outputHandlesByName().values());
    return new PendingTask(blobDefinitions, blobInfos, blobHandles, inputReadinessByHandle(allocation, inputReadiness), allocation, allInputHandlesByName, deferredTaskCreation, payloadUpload, taskInfo, taskHandle);
  }

  /**
   * Watches the outputs of a task once the cluster has accepted the task. If the submission fails,
   * no task will ever produce the outputs, so they are reported to the listener as failed instead
   * of being watched. Until then, the coordinator counts them as in flight, so
   * {@link #waitUntilFinished()} waits for the submission.
   */
  private void watchOutputsOnceSubmitted(CompletionStage<TaskInfo> taskInfo, Collection<BlobHandle> outputHandles) {
    if (outputHandles.isEmpty()) return;

    var outputs = List.copyOf(outputHandles);
    blobCompletionCoordinator.track(taskInfo.handle((info, ex) -> ex).thenCompose(ex -> {
      if (ex != null) return reportFailedOutputs(outputs, ex instanceof CompletionException ? ex.getCause() : ex);
      blobCompletionCoordinator.enqueue(outputs);
      return completedFuture(null);
    }));
  }

  /**
   * Calls {@link BlobCompletionListener#onError} for each output that was created. An output whose
   * metadata creation failed has no id to report, and its {@link BlobHandle#deferredBlobInfo()}
   * already carries the failure.
   */
  private CompletionStage<Void> reportFailedOutputs(List<BlobHandle> outputs, Throwable cause) {
    return CompletableFuture.allOf(outputs.stream()
                                          .map(output -> output.deferredBlobInfo()
                                                               .thenAccept(blobInfo -> notifyFailedOutput(blobInfo, cause))
                                                               .exceptionally(ex -> null)
                                                               .toCompletableFuture())
                                          .toArray(CompletableFuture[]::new));
  }

  private void notifyFailedOutput(BlobInfo blobInfo, Throwable cause) {
    try {
      outputListener.onError(new BlobCompletionListener.BlobError(blobInfo, cause));
    } catch (Throwable throwable) {
      logger.error("Listener threw exception. Session id:{}, blob id:{}", sessionId.asString(), blobInfo.id().asString(), throwable);
    }
  }

  /**
   * Adds tasks to the buffer, sends every full batch, and arms the window timer if tasks remain.
   * <p>
   * The timer runs on the scheduler shared by every session, so it only hands the flush over to
   * the client executor: sending can block while waiting for a channel.
   */
  private void enqueue(List<PendingTask> pendingTasks) {
    var batches = new ArrayList<List<PendingTask>>();
    synchronized (lock) {
      buffer.addAll(pendingTasks);
      while (buffer.size() >= batching.batchSize()) {
        batches.add(drain(batching.batchSize()));
      }
      if (!buffer.isEmpty()) {
        if (batching.window().isZero()) {
          batches.add(drain(buffer.size()));
        } else if (timer == null) {
          timer = scheduler.schedule(() -> flushExecutor.execute(this::flush), batching.window().toNanos(), NANOSECONDS);
        }
      }
    }
    batches.forEach(this::send);
  }

  /**
   * Removes the first {@code count} buffered tasks, and cancels the timer when the buffer becomes empty.
   * Must be called while holding {@link #lock}.
   */
  private List<PendingTask> drain(int count) {
    var batch = new ArrayList<>(buffer.subList(0, count));
    buffer.subList(0, count).clear();
    if (buffer.isEmpty() && timer != null) {
      timer.cancel(false);
      timer = null;
    }
    return batch;
  }

  /**
   * Creates the blob metadata of a batch with one {@code CreateResultsMetaData} call per
   * {@value #MAX_BLOBS_PER_REQUEST} blobs, then submits its tasks with one {@code SubmitTasks}
   * call per {@value #MAX_TASKS_PER_REQUEST} tasks.
   * <p>
   * If starting a call throws, for instance because the channel pool is closed, every task and
   * blob of the batch fails with the exception instead of staying pending.
   *
   * @return a stage that completes, never exceptionally, when every task of the batch is acknowledged or failed
   */
  private CompletionStage<Void> send(List<PendingTask> batch) {
    if (batch.isEmpty()) return completedFuture(null);
    logger.debug("Sending {} buffered tasks. Session id:{}", batch.size(), sessionId.asString());

    resultsServiceConfiguration.dataChunkMaxSize().thenAccept(dataChunkMaxSize -> dispatch(batch, dataChunkMaxSize));

    return CompletableFuture.allOf(batch.stream()
                                        .map(pendingTask -> pendingTask.taskInfo().handle((info, ex) -> null))
                                        .toArray(CompletableFuture[]::new));
  }

  private static Map<BlobHandle, CompletableFuture<Void>> inputReadinessByHandle(BlobHandlesAllocation allocation, List<CompletableFuture<Void>> readiness) {
    var handles = new ArrayList<>(allocation.inputHandlesByName().values());
    var byHandle = new IdentityHashMap<BlobHandle, CompletableFuture<Void>>();
    for (int i = 0; i < handles.size(); i++) byHandle.put(handles.get(i), readiness.get(i));
    return byHandle;
  }

  private void dispatch(List<PendingTask> batch, int dataChunkMaxSize) {
    try {
      createBlobs(batch, dataChunkMaxSize);
      for (int start = 0; start < batch.size(); start += MAX_TASKS_PER_REQUEST) {
        submitTaskCreations(batch.subList(start, Math.min(start + MAX_TASKS_PER_REQUEST, batch.size())));
      }
    } catch (RuntimeException exception) {
      logger.error("Sending {} tasks failed. Session id:{}", batch.size(), sessionId.asString(), exception);
      batch.forEach(pendingTask -> {
        pendingTask.blobInfos().forEach(blobInfo -> blobInfo.completeExceptionally(exception));
        pendingTask.inputReadiness().values().forEach(ready -> ready.completeExceptionally(exception));
        pendingTask.payloadUpload().completeExceptionally(exception);
        pendingTask.taskInfo().completeExceptionally(exception);
      });
    }
  }

  /**
   * Creates the blobs of a batch of tasks.
   * <p>
   * With a positive {@code dataChunkMaxSize}, input blobs held in memory and payloads go with their
   * data in {@code CreateResults} calls, each request staying within {@code dataChunkMaxSize} bytes;
   * outputs and larger inputs get their metadata in {@code CreateResultsMetaData} calls, then the
   * inputs are uploaded as streams. A payload names the ids of its task's inputs and outputs, so it
   * is created once they are known. With 0, every blob takes the metadata-then-upload path.
   */
  private void createBlobs(List<PendingTask> batch, int dataChunkMaxSize) {
    var inline = dataChunkMaxSize > 0;
    var metadataDefinitions = new ArrayList<BlobDefinition>();
    var metadataBlobInfos = new ArrayList<CompletableFuture<BlobInfo>>();
    var inlineBlobs = new ArrayList<InlineBlob>();
    var streamedInputs = new ArrayList<StreamedInput>();

    for (var pendingTask : batch) {
      var definitions = pendingTask.blobDefinitions();
      for (int i = 0; i < definitions.size(); i++) {
        var definition = definitions.get(i);
        var blobInfo = pendingTask.blobInfos().get(i);
        if (i == 0) {
          // The payload: its content is only known once the ids of the inputs and outputs are.
          if (!inline) {
            metadataDefinitions.add(definition);
            metadataBlobInfos.add(blobInfo);
          }
          continue;
        }
        var resultCreate = inline && definition instanceof InputBlobDefinition input ? inlineResultCreate(input.name(), input.manualDeletion(), input.data(), dataChunkMaxSize) : null;
        var handle = pendingTask.blobHandles().get(i);
        var ready = pendingTask.inputReadiness().get(handle);
        if (resultCreate != null) {
          inlineBlobs.add(new InlineBlob(resultCreate, blobInfo));
          if (ready != null) blobInfo.whenComplete((info, error) -> completeWith(ready, error));
        } else {
          metadataDefinitions.add(definition);
          metadataBlobInfos.add(blobInfo);
          if (definition instanceof InputBlobDefinition input) {
            streamedInputs.add(new StreamedInput(handle, input.data(), ready));
          }
        }
      }
    }

    createMetadata(metadataDefinitions, metadataBlobInfos);
    createWithData(inlineBlobs, dataChunkMaxSize);
    streamedInputs.forEach(input -> {
      var upload = input.handle().uploadData(input.data());
      if (input.ready() != null) upload.whenComplete((ignored, error) -> completeWith(input.ready(), error));
    });

    if (inline) {
      createPayloads(batch, dataChunkMaxSize);
    } else {
      batch.forEach(pendingTask -> uploadPayload(pendingTask.allInputHandlesByName(), pendingTask.allocation())
        .whenComplete((ignored, error) -> completeWith(pendingTask.payloadUpload(), error)));
    }
  }

  /**
   * Creates the payloads of a batch once the ids they name are known, with their data when they fit
   * in a request, and through metadata then upload otherwise.
   */
  private void createPayloads(List<PendingTask> batch, int dataChunkMaxSize) {
    var payloads = batch.stream()
                        .map(pendingTask -> payloadData(pendingTask.allInputHandlesByName(), pendingTask.allocation()).toCompletableFuture())
                        .toList();

    CompletableFuture.allOf(payloads.stream().map(payload -> payload.handle((data, error) -> null)).toArray(CompletableFuture[]::new))
                     .thenRun(() -> {
                       try {
                         var inlinePayloads = new ArrayList<InlineBlob>();
                         var metadataDefinitions = new ArrayList<BlobDefinition>();
                         var metadataBlobInfos = new ArrayList<CompletableFuture<BlobInfo>>();
                         var streamedPayloads = new ArrayList<Map.Entry<PendingTask, byte[]>>();

                         for (int i = 0; i < batch.size(); i++) {
                           var pendingTask = batch.get(i);
                           var payloadBlobInfo = pendingTask.blobInfos().get(0);
                           var payload = payloads.get(i);
                           if (payload.isCompletedExceptionally()) {
                             var error = payload.handle((data, ex) -> unwrap(ex)).join();
                             payloadBlobInfo.completeExceptionally(error);
                             pendingTask.payloadUpload().completeExceptionally(error);
                             continue;
                           }
                           var data = payload.join();
                           var resultCreate = inlineResultCreate(PAYLOAD_NAME, false, InMemoryBlobData.from(data), dataChunkMaxSize);
                           if (resultCreate != null) {
                             inlinePayloads.add(new InlineBlob(resultCreate, payloadBlobInfo));
                             payloadBlobInfo.whenComplete((info, error) -> completeWith(pendingTask.payloadUpload(), error));
                           } else {
                             metadataDefinitions.add(pendingTask.blobDefinitions().get(0));
                             metadataBlobInfos.add(payloadBlobInfo);
                             streamedPayloads.add(Map.entry(pendingTask, data));
                           }
                         }

                         createWithData(inlinePayloads, dataChunkMaxSize);
                         createMetadata(metadataDefinitions, metadataBlobInfos);
                         streamedPayloads.forEach(entry -> entry.getKey()
                                                                .allocation()
                                                                .payloadHandle()
                                                                .uploadData(InMemoryBlobData.from(entry.getValue()))
                                                                .whenComplete((ignored, error) -> completeWith(entry.getKey().payloadUpload(), error)));
                       } catch (RuntimeException exception) {
                         // The pool may have closed meanwhile: fail the payloads instead of losing the exception.
                         logger.error("Creating the payloads of {} tasks failed. Session id:{}", batch.size(), sessionId.asString(), exception);
                         batch.forEach(pendingTask -> {
                           pendingTask.blobInfos().get(0).completeExceptionally(exception);
                           pendingTask.payloadUpload().completeExceptionally(exception);
                         });
                       }
                     });
  }

  /**
   * Builds the {@code CreateResults} entry of a blob, or returns null when the blob must be uploaded as a
   * stream: its data is not held in memory, or a request carrying it alone would exceed {@code dataChunkMaxSize}.
   */
  private CreateResultsRequest.ResultCreate inlineResultCreate(String name, boolean manualDeletion, BlobData data, int dataChunkMaxSize) {
    if (!(data instanceof InMemoryBlobData inMemory)) return null;

    var resultCreate = CreateResultsRequest.ResultCreate.newBuilder()
                                           .setName(name)
                                           .setManualDeletion(manualDeletion)
                                           .setData(UnsafeByteOperations.unsafeWrap(inMemory.data()))
                                           .build();
    return sessionIdSize() + entrySize(resultCreate) <= dataChunkMaxSize ? resultCreate : null;
  }

  /**
   * Creates blobs with their data, packing them in order into {@code CreateResults} requests of at most
   * {@code dataChunkMaxSize} bytes and {@link #MAX_BLOBS_PER_REQUEST} blobs.
   */
  private void createWithData(List<InlineBlob> blobs, int dataChunkMaxSize) {
    var group = new ArrayList<InlineBlob>();
    long groupSize = sessionIdSize();
    for (var blob : blobs) {
      var size = entrySize(blob.resultCreate());
      if (!group.isEmpty() && (groupSize + size > dataChunkMaxSize || group.size() >= MAX_BLOBS_PER_REQUEST)) {
        sendCreateResults(group);
        group = new ArrayList<>();
        groupSize = sessionIdSize();
      }
      group.add(blob);
      groupSize += size;
    }
    if (!group.isEmpty()) sendCreateResults(group);
  }

  /**
   * Sends one {@code CreateResults} call for a group, retried on transient failures with the pool's
   * retry policy, as the uploads it replaces are. A retry after a lost response may leave the results
   * of the first attempt on the server, unused by any task: they are harmless orphans of the session.
   */
  private void sendCreateResults(List<InlineBlob> group) {
    var request = CreateResultsRequest.newBuilder()
                                      .setSessionId(sessionId.asString())
                                      .addAllResults(group.stream().map(InlineBlob::resultCreate).toList())
                                      .build();
    RetryableStreamOperation.execute(
                              () -> callAsync(channel -> Futures.toCompletionStage(ResultsGrpc.newFutureStub(channel).createResults(request))),
                              channelPool.retryPolicy())
                            .thenApply(CreateResultsResponse::getResultsList)
                            .whenComplete((resultRaws, ex) -> {
                              var error = ex;
                              if (error == null && resultRaws.size() != group.size()) {
                                error = new ArmoniKException("CreateResults returned " + resultRaws.size() + " results for "
                                  + group.size() + " requested. Session id:" + sessionId.asString());
                              }
                              for (int i = 0; i < group.size(); i++) {
                                if (error != null) group.get(i).blobInfo().completeExceptionally(error);
                                else group.get(i).blobInfo().complete(toBlobInfo(sessionId, resultRaws.get(i)));
                              }
                            });
  }

  private int sessionIdSize() {
    return CodedOutputStream.computeStringSize(CreateResultsRequest.SESSION_ID_FIELD_NUMBER, sessionId.asString());
  }

  private static int entrySize(CreateResultsRequest.ResultCreate resultCreate) {
    return CodedOutputStream.computeMessageSize(CreateResultsRequest.RESULTS_FIELD_NUMBER, resultCreate);
  }

  /**
   * Runs a call through the pool, turning a synchronous failure, such as a closed pool, into a failed stage.
   */
  private <T> CompletionStage<T> callAsync(Function<ManagedChannel, CompletionStage<T>> call) {
    try {
      return channelPool.executeAsync(call);
    } catch (RuntimeException exception) {
      return CompletableFuture.failedFuture(exception);
    }
  }

  private static void completeWith(CompletableFuture<Void> future, Throwable error) {
    if (error == null) future.complete(null);
    else future.completeExceptionally(unwrap(error));
  }

  private void createMetadata(List<BlobDefinition> definitions, List<CompletableFuture<BlobInfo>> blobInfos) {
    for (int start = 0; start < definitions.size(); start += MAX_BLOBS_PER_REQUEST) {
      var end = Math.min(start + MAX_BLOBS_PER_REQUEST, definitions.size());
      var groupDefinitions = new ArrayList<>(definitions.subList(start, end));
      var groupBlobInfos = blobInfos.subList(start, end);
      callAsync(createMetadataCall(groupDefinitions))
                 .thenApply(CreateResultsMetaDataResponse::getResultsList)
                 .whenComplete((resultRaws, ex) -> {
                   for (int i = 0; i < groupBlobInfos.size(); i++) {
                     if (ex != null) groupBlobInfos.get(i).completeExceptionally(ex);
                     else groupBlobInfos.get(i).complete(toBlobInfo(sessionId, resultRaws.get(i)));
                   }
                 });
    }
  }

  private Function<ManagedChannel, CompletionStage<CreateResultsMetaDataResponse>> createMetadataCall(List<BlobDefinition> allDefinitions) {
    return channel -> Futures.toCompletionStage(ResultsGrpc.newFutureStub(channel).createResultsMetaData(toResultMetaDataRequest(sessionId, allDefinitions)));
  }

  /**
   * Submits a group of tasks with one {@code SubmitTasks} call, once every task of the group is
   * ready to run or known never to run.
   * <p>
   * A task is ready once its payload is uploaded and each of its inputs is ready (see
   * {@link BlobHandle#deferredReadiness()}): its uploads are done, and the tasks producing the
   * other inputs were accepted. The cluster would accept a task with a missing input but never run
   * it, so such a task fails here instead of being sent. Inputs produced by a task of the same
   * group cannot be awaited, since both are sent in the same call: the consumer of a failed task of
   * the group fails as well. The other tasks of the group are still sent.
   * <p>
   * The cluster returns the task infos in request order, so the i-th task info belongs to the i-th
   * task sent.
   */
  private void submitTaskCreations(List<PendingTask> group) {
    var producedInGroup = Collections.newSetFromMap(new IdentityHashMap<BlobHandle, Boolean>());
    group.forEach(pendingTask -> producedInGroup.addAll(pendingTask.taskHandle().outputs().values()));

    var readyCreations = group.stream().map(pendingTask -> readyTaskCreation(pendingTask, producedInGroup)).toList();
    CompletableFuture.allOf(readyCreations.toArray(CompletableFuture[]::new))
                     .handle((ignored, ex) -> null)
                     .thenAccept(ignored -> submitReadyTasks(group, readyCreations));
  }

  /**
   * Returns the task creation of a task once its payload is uploaded and its inputs that are not
   * produced in its group are ready, or a failed stage naming the blob that will never be available.
   */
  private CompletableFuture<TaskCreation> readyTaskCreation(PendingTask pendingTask, Set<BlobHandle> producedInGroup) {
    var readiness = new ArrayList<CompletableFuture<Void>>();
    readiness.add(failingWith(pendingTask.payloadUpload(), "The payload of the task could not be uploaded"));
    pendingTask.taskHandle().inputs().forEach((name, input) -> {
      if (!producedInGroup.contains(input)) {
        readiness.add(failingWith(input.deferredReadiness(), "Input '" + name + "' of the task will never be available"));
      }
    });
    return CompletableFuture.allOf(readiness.toArray(CompletableFuture[]::new))
                            .thenCompose(ignored -> pendingTask.deferredTaskCreation());
  }

  private static CompletableFuture<Void> failingWith(CompletionStage<Void> stage, String message) {
    return stage.handle((ignored, ex) -> {
      if (ex != null) throw new CompletionException(new ArmoniKException(message, unwrap(ex)));
      return (Void) null;
    }).toCompletableFuture();
  }

  /**
   * Fails the tasks of the group that will never run, in request order so that a failure reaches
   * the consumers of the failed task's outputs, and submits the others with one {@code SubmitTasks} call.
   */
  private void submitReadyTasks(List<PendingTask> group, List<CompletableFuture<TaskCreation>> readyCreations) {
    var failedOutputs = Collections.newSetFromMap(new IdentityHashMap<BlobHandle, Boolean>());
    var toSend = new ArrayList<PendingTask>();
    var taskCreations = new ArrayList<TaskCreation>();
    for (int i = 0; i < group.size(); i++) {
      var pendingTask = group.get(i);
      var cause = readyCreations.get(i).handle((creation, ex) -> ex == null ? null : unwrap(ex)).join();
      if (cause == null) {
        cause = pendingTask.taskHandle()
                           .inputs()
                           .entrySet()
                           .stream()
                           .filter(input -> failedOutputs.contains(input.getValue()))
                           .findFirst()
                           .map(input -> (Throwable) new ArmoniKException("Input '" + input.getKey() + "' of the task is an output of a task that was not submitted"))
                           .orElse(null);
      }
      if (cause != null) {
        logger.warn("Task not submitted: {}. Session id:{}", cause.getMessage(), sessionId.asString());
        failedOutputs.addAll(pendingTask.taskHandle().outputs().values());
        pendingTask.taskInfo().completeExceptionally(cause);
      } else {
        toSend.add(pendingTask);
        taskCreations.add(readyCreations.get(i).join());
      }
    }
    if (toSend.isEmpty()) return;

    try {
      submitTasks(TaskMapper.toSubmitTasksRequest(sessionId, defaultTaskConfiguration, taskCreations))
        .whenComplete((response, ex) -> {
          if (ex != null) {
            logger.error("Submission of {} tasks failed. Session id:{}", toSend.size(), sessionId.asString(), ex);
            toSend.forEach(pendingTask -> pendingTask.taskInfo().completeExceptionally(ex));
          } else {
            for (int i = 0; i < toSend.size(); i++) {
              toSend.get(i).taskInfo().complete(new TaskInfo(TaskId.from(response.getTaskInfos(i).getTaskId())));
            }
          }
        });
    } catch (RuntimeException exception) {
      logger.error("Submission of {} tasks failed. Session id:{}", toSend.size(), sessionId.asString(), exception);
      toSend.forEach(pendingTask -> pendingTask.taskInfo().completeExceptionally(exception));
    }
  }

  private static Throwable unwrap(Throwable throwable) {
    return throwable instanceof CompletionException && throwable.getCause() != null ? throwable.getCause() : throwable;
  }

  private CompletionStage<SubmitTasksResponse> submitTasks(SubmitTasksRequest request) {
    logger.debug("Submitting {} tasks. Session id:{}", request.getTaskCreationsCount(), sessionId.asString());
    return channelPool.executeAsync(channel -> Futures.toCompletionStage(TasksGrpc.newFutureStub(channel).submitTasks(request)));
  }

  private CompletionStage<Void> uploadPayload(Map<String, BlobHandle> allInputHandlesByName, BlobHandlesAllocation allocation) {
    return payloadData(allInputHandlesByName, allocation)
      .thenCompose(data -> allocation.payloadHandle().uploadData(InMemoryBlobData.from(data)));
  }

  private CompletionStage<byte[]> payloadData(Map<String, BlobHandle> allInputHandlesByName, BlobHandlesAllocation allocation) {
    var inputIdByName = allInputHandlesByName.entrySet().stream().collect(ids());
    var outputIdByName = allocation.outputHandlesByName().entrySet().stream().collect(ids());

    return Futures.allOf(inputIdByName)
                  .thenCombine(Futures.allOf(outputIdByName), this::serializeToJson);
  }

  private byte[] serializeToJson(Map<String, BlobId> inputIds, Map<String, BlobId> outputIds) {
    var in = inputIds.entrySet()
                     .stream()
                     .collect(toMap(Map.Entry::getKey, e -> e.getValue().asString()));
    var out = outputIds.entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, e -> e.getValue().asString()));

    var payload = Map.of("inputs", in, "outputs", out);
    return gson.toJson(payload).getBytes(UTF_8);
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
   * A task waiting in the submission buffer: the definitions of its blobs and the blob infos to
   * complete once their metadata is created, its deferred task creation, the upload of its
   * payload, and its handle.
   */
  private record PendingTask(List<BlobDefinition> blobDefinitions,
                             List<CompletableFuture<BlobInfo>> blobInfos,
                             List<BlobHandle> blobHandles,
                             Map<BlobHandle, CompletableFuture<Void>> inputReadiness,
                             BlobHandlesAllocation allocation,
                             Map<String, BlobHandle> allInputHandlesByName,
                             CompletionStage<TaskCreation> deferredTaskCreation,
                             CompletableFuture<Void> payloadUpload,
                             CompletableFuture<TaskInfo> taskInfo,
                             TaskHandle taskHandle) {
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
   * @see #prepare(TaskDefinition)
   */
  private record StreamedInput(BlobHandle handle, BlobData data, CompletableFuture<Void> ready) {
  }

  private record InlineBlob(CreateResultsRequest.ResultCreate resultCreate, CompletableFuture<BlobInfo> blobInfo) {
  }

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
    BlobId payloadId,
    List<BlobId> inputBlobIds,
    List<BlobId> outputBlobIds,
    TaskConfiguration sessionTaskConfig,
    TaskConfiguration taskConfig,
    Map<String, String> workerLibraryOptions
  ) {

    TaskSubmissionData(BlobId payloadId) {
      this(payloadId, List.of(), List.of(), null, null, Map.of());
    }

    TaskSubmissionData withInputIds(List<BlobId> inputBlobIds) {
      return new TaskSubmissionData(payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, workerLibraryOptions);
    }

    TaskSubmissionData withOutputIds(List<BlobId> outputBlobIds) {
      return new TaskSubmissionData(payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, workerLibraryOptions);
    }

    TaskSubmissionData withSessionTaskConfig(TaskConfiguration sessionTaskConfig) {
      return new TaskSubmissionData(payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, workerLibraryOptions);
    }

    TaskSubmissionData withTaskConfig(TaskConfiguration taskConfig) {
      return new TaskSubmissionData(payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, workerLibraryOptions);
    }

    TaskSubmissionData withWorkerLibraryOptions(Map<String, String> workerLibraryOptions) {
      var effectiveOptions = workerLibraryOptions != null ? workerLibraryOptions : Map.<String, String>of();
      return new TaskSubmissionData(payloadId, inputBlobIds, outputBlobIds, sessionTaskConfig, taskConfig, effectiveOptions);
    }

    TaskCreation toTaskCreation() {
      var effectiveTaskConfig = taskConfig != null ?
        taskConfig.withOptions(workerLibraryOptions) :
        sessionTaskConfig.withOptions(workerLibraryOptions);

      var dataDependencies = new ArrayList<>(inputBlobIds);
      if (!workerLibraryOptions.isEmpty()) {
        dataDependencies.add(BlobId.from(workerLibraryOptions.get(LIBRARY_BLOB_ID)));
      }

      return TaskMapper.toTaskCreation(dataDependencies, outputBlobIds, payloadId, effectiveTaskConfig);
    }
  }
}
