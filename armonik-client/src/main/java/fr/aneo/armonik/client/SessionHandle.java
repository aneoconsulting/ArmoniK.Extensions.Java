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

import fr.aneo.armonik.api.grpc.v1.results.ResultsGrpc;
import fr.aneo.armonik.api.grpc.v1.sessions.SessionsGrpc;
import fr.aneo.armonik.client.definition.SessionDefinition;
import fr.aneo.armonik.client.definition.TaskDefinition;
import fr.aneo.armonik.client.definition.blob.BlobDefinition;
import fr.aneo.armonik.client.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.client.internal.concurrent.Futures;
import io.grpc.ManagedChannel;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static fr.aneo.armonik.client.internal.grpc.mappers.BlobMapper.toResultMetaDataRequest;
import static fr.aneo.armonik.client.internal.grpc.mappers.SessionMapper.*;
import static java.util.Objects.requireNonNull;

/**
 * Handle representing a session within the ArmoniK distributed computing platform.
 * <p>
 * A session is a logical container for tasks and associated data within the ArmoniK cluster [[1]](https://armonik.readthedocs.io/en/latest/content/armonik/glossary.html).
 * This handle provides the primary interface for task submission and session lifecycle management.
 * <p>
 * SessionHandle instances are responsible for:
 * <ul>
 *   <li>Submitting tasks within the session context</li>
 *   <li>Creating and managing session-scoped blob handles</li>
 *   <li>Managing task output completion events and callbacks</li>
 *   <li>Coordinating session-scoped operations</li>
 * </ul>
 * <p>
 * This class is thread-safe and supports concurrent operations.
 *
 * @see TaskHandle
 * @see TaskDefinition
 * @see SessionInfo
 */
public final class SessionHandle {

  private final SessionInfo sessionInfo;
  private final TaskSubmitter taskSubmitter;
  private final ChannelPool channelPool;


  /**
   * Constructs a session handle for an existing or newly created session in the ArmoniK cluster.
   * <p>
   * It initializes the handle with the session metadata and configures task submission
   * and optional output blob monitoring using the provided {@link BlobCompletionListener}.
   * <p>
   * If an output listener is supplied, this handle automatically begins monitoring
   * output blob completions using the default batching strategy (not exposed in the public API).
   *
   * @param sessionInfo    immutable metadata describing the session, including the session ID
   * @param outputListener optional listener notified when output blobs reach completion; may be {@code null}
   * @param channelPool    the gRPC channel pool used for communication with the cluster
   * @throws NullPointerException if {@code sessionInfo} or {@code channelPool} is {@code null}
   * @see SessionInfo
   * @see BlobCompletionListener
   * @see ArmoniKClient#openSession(SessionDefinition)
   * @see ArmoniKClient#getSession(SessionId, BlobCompletionListener)
   */
  SessionHandle(SessionInfo sessionInfo, BlobCompletionListener outputListener, ChannelPool channelPool) {
    this.sessionInfo = requireNonNull(sessionInfo, "sessionInfo must not be null");
    this.taskSubmitter = new TaskSubmitter(sessionInfo.id(), sessionInfo.taskConfiguration(), outputListener, channelPool);
    this.channelPool = requireNonNull(channelPool, "channelPool must not be null");
  }

  /**
   * Returns the immutable session information associated with this handle.
   * <p>
   * The session information includes the unique session identifier, partition IDs,
   * and default task configuration that applies to tasks submitted within this session.
   *
   * @return the session metadata
   * @see SessionInfo
   */
  public SessionInfo sessionInfo() {
    return sessionInfo;
  }

  /**
   * Submits a new task for execution within this session.
   * <p>
   * This method creates and submits a task based on the provided task definition.
   * The task will inherit default configurations from the session unless explicitly
   * overridden in the task definition. Input blobs specified in the task definition
   * will be uploaded to the cluster, and output blob handles will be created for
   * expected outputs.
   * <p>
   * The returned {@link TaskHandle} provides access to task metadata and associated
   * input/output blobs. Task submission is asynchronous - the handle is returned
   * immediately while the actual submission continues in the background.
   *
   * @param taskDefinition the definition specifying task inputs, expected outputs,
   *                       payload, and optional task-specific configuration
   * @return a handle representing the submitted task
   * @throws NullPointerException if taskDefinition is null
   * @throws RuntimeException     if task submission fails due to cluster communication issues
   * @see TaskHandle
   * @see TaskDefinition
   */
  public TaskHandle submitTask(TaskDefinition taskDefinition) {
    return taskSubmitter.submit(taskDefinition);
  }

  /**
   * Blocks until all currently pending task outputs are completed and the configured
   * task output listener has been invoked for each of them.
   * <p>
   * This method acts as a synchronization barrier for output processing. It takes a
   * snapshot of the outputs being watched at the moment of invocation and blocks the
   * calling thread until:
   * <ul>
   *   <li>each output has reached a terminal state (completed or failed), and</li>
   *   <li>the task output listener has been applied for each terminal output.</li>
   * </ul>
   * Outputs submitted or started after this method is called are not included in this wait.
   * If there are no pending outputs at call time, the method returns immediately.
   * <p>
   * This method does not cancel any ongoing operations and is safe to call concurrently.
   * Each concurrent call waits for the set of outputs that were in-flight at the time
   * of that specific invocation.
   *
   * @see BlobCompletionListener
   */
  public void awaitOutputsProcessed() {
    taskSubmitter.waitUntilFinished().toCompletableFuture().join();
  }

  @Override
  public String toString() {
    return "SessionHandle{" +
      "sessionInfo=" + sessionInfo +
      '}';
  }

  /**
   * Creates a new blob handle within this session and uploads the provided data.
   * <p>
   * This method creates a session-scoped blob that can be shared across multiple tasks
   * within the same session. The blob is immediately allocated in the ArmoniK cluster
   * and the data is uploaded asynchronously. The returned {@link BlobHandle} can be
   * referenced by multiple tasks as input data, avoiding duplicate uploads of the same content.
   * <p>
   * Session-scoped blobs are useful for:
   * <ul>
   *   <li><strong>Shared input data:</strong> Common datasets or configuration files used by multiple tasks</li>
   *   <li><strong>Reference data:</strong> Lookup tables or static resources accessed by various tasks</li>
   *   <li><strong>Intermediate results:</strong> Data produced by one task and consumed by others</li>
   * </ul>
   * <p>
   * The upload operation begins immediately and continues asynchronously in the background.
   * The blob handle is returned immediately, allowing it to be referenced by task definitions
   * even while the upload is still in progress.
   *
   * @param blobDefinition the definition containing the data to upload and store as a shared blob
   * @return a handle representing the created blob within this session
   * @throws NullPointerException if blobDefinition is null
   * @throws RuntimeException     if blob creation or upload fails due to cluster communication issues
   * @see BlobHandle
   * @see InputBlobDefinition
   * @see TaskDefinition#withInput(String, BlobHandle)
   */
  public BlobHandle createBlob(InputBlobDefinition blobDefinition) {
    requireNonNull(blobDefinition, "blobDefinition must not be null");

    var deferredBlobInfo = channelPool.executeAsync(createBlobInfo(blobDefinition));
    var blobHandle = new BlobHandle(sessionInfo.id(), deferredBlobInfo, channelPool);
    blobHandle.uploadData(blobDefinition.data());
    return blobHandle;
  }

  /**
   * Requests cancellation of this session in the ArmoniK cluster.
   * <p>
   * Cancelling a session signals the control plane to stop all remaining work associated
   * with this session. Depending on the server configuration and current task states,
   * running tasks may be interrupted and queued tasks will no longer be scheduled.
   * Results that have already been produced remain accessible unless the session
   * is subsequently purged or deleted.
   * <p>
   * The returned completion stage is completed asynchronously with the updated
   * {@link SessionState} once the cancellation request has been processed by the
   * Sessions service, or completed exceptionally if the request fails.
   *
   * @return a completion stage that yields the updated state of this session after
   *         the cancellation request has been applied
   *
   * @see SessionState
   */
  public CompletionStage<SessionState> cancel() {
    return channelPool.executeAsync(channel -> {
      var sessionsFutureStub = SessionsGrpc.newFutureStub(channel);
      return Futures.toCompletionStage(sessionsFutureStub.cancelSession(toCancelSessionRequest(sessionInfo.id())))
                    .thenApply(response -> toSessionState(response.getSession()));
    });
  }

  /**
   * Pauses this session in the ArmoniK cluster.
   * <p>
   * Pausing a session temporarily suspends the scheduling of new tasks in the session.
   * Tasks that are already running continue until completion, but pending tasks are
   * not picked up for execution until the session is resumed via {@link #resume()}.
   * <p>
   * This operation is useful when you need to temporarily throttle or stop processing
   * without cancelling the session or losing its state.
   * The returned completion stage is completed asynchronously with the updated
   * {@link SessionState} once the pause request has been processed by the Sessions
   * service, or completed exceptionally if the request fails.
   *
   * @return a completion stage that yields the updated state of this session after
   *         the pause request has been applied
   *
   * @see #resume()
   * @see SessionState
   */
  public CompletionStage<SessionState> pause() {
    return channelPool.executeAsync(channel -> {
      var sessionsFutureStub = SessionsGrpc.newFutureStub(channel);
      return Futures.toCompletionStage(sessionsFutureStub.pauseSession(toPauseSessionRequest(sessionInfo.id())))
                    .thenApply(response -> toSessionState(response.getSession()));
    });
  }
  /**
   * Resumes a previously paused session in the ArmoniK cluster.
   * <p>
   * Resuming a session re-enables scheduling of tasks that were previously held
   * while the session was paused. Any pending tasks become eligible for execution
   * again according to the cluster scheduling policies.
   * <p>
   * Calling this method on a session that is not paused is safe; the Sessions service
   * will simply return the current state of the session.
   * The returned completion stage is completed asynchronously with the updated
   * {@link SessionState} once the resume request has been processed by the Sessions
   * service, or completed exceptionally if the request fails.
   *
   * @return a completion stage that yields the updated state of this session after
   *         the resume request has been applied
   *
   * @see #pause()
   * @see SessionState
   */
  public CompletionStage<SessionState> resume() {
    return channelPool.executeAsync(channel -> {
      var sessionsFutureStub = SessionsGrpc.newFutureStub(channel);
      return Futures.toCompletionStage(sessionsFutureStub.resumeSession(toResumeSessionRequest(sessionInfo.id())))
                    .thenApply(response -> toSessionState(response.getSession()));
    });
  }

  /**
   * Closes this session in the ArmoniK cluster.
   * <p>
   * Closing a session finalizes it and prevents any new task submissions, while
   * preserving existing tasks, results, and metadata. This is the recommended way
   * to indicate that no further work will be submitted for this session once all
   * expected tasks have been created.
   * <p>
   * Closing a session does not remove its data. To free up storage or completely
   * remove the session, combine this operation with {@link #purge()} and
   * {@link #delete()} as appropriate.
   * The returned completion stage is completed asynchronously with the updated
   * {@link SessionState} once the close request has been processed by the Sessions
   * service, or completed exceptionally if the request fails.
   *
   * @return a completion stage that yields the updated state of this session after
   *         the close request has been applied
   *
   * @see #purge()
   * @see #delete()
   * @see SessionState
   */
  public CompletionStage<SessionState> close() {
    return channelPool.executeAsync(channel -> {
      var sessionsFutureStub = SessionsGrpc.newFutureStub(channel);
      return Futures.toCompletionStage(sessionsFutureStub.closeSession(toCloseSessionRequest(sessionInfo.id())))
                    .thenApply(response -> toSessionState(response.getSession()));
    });
  }

  /**
   * Purges this session's data in the ArmoniK cluster.
   * <p>
   * Purging a session removes the underlying data for its blobs (task inputs and
   * outputs) from the storage layer while keeping the session and task metadata.
   * This operation is useful to reclaim storage space once the actual data is no
   * longer needed but you still want to keep an audit trail or execution history.
   * <p>
   * After a purge, attempts to download blob data associated with this session
   * will fail, but session and task information remain available until the session
   * is deleted.
   * The returned completion stage is completed asynchronously with the updated
   * {@link SessionState} once the purge request has been processed by the Sessions
   * service, or completed exceptionally if the request fails.
   *
   * @return a completion stage that yields the updated state of this session after
   *         the purge request has been applied
   *
   * @see #delete()
   * @see SessionState
   */
  public CompletionStage<SessionState> purge() {
    return channelPool.executeAsync(channel -> {
      var sessionsFutureStub = SessionsGrpc.newFutureStub(channel);
      return Futures.toCompletionStage(sessionsFutureStub.purgeSession(toPurgeSessionRequest(sessionInfo.id())))
                    .thenApply(response -> toSessionState(response.getSession()));
    });
  }

  /**
   * Deletes this session from the ArmoniK cluster.
   * <p>
   * Deleting a session permanently removes its metadata from the Sessions, Tasks,
   * and Blobs. This is typically the final step in the lifecycle of a
   * session once it has been closed and, optionally, purged.
   * <p>
   * After deletion, this handle still exists as a local object but any further
   * interaction with the remote session (such as submitting tasks or querying
   * state) will fail because the session no longer exists in the cluster.
   * Clients are expected to discard the handle after deletion.
   * The returned completion stage is completed asynchronously with the updated
   * {@link SessionState} reported by the Sessions service just before removal,
   * or completed exceptionally if the request fails.
   *
   * @return a completion stage that yields the last known state of this session
   *         before it is removed from the cluster
   *
   * @see #close()
   * @see #purge()
   * @see SessionState
   */
  public CompletionStage<SessionState> delete() {
    return channelPool.executeAsync(channel -> {
      var sessionsFutureStub = SessionsGrpc.newFutureStub(channel);
      return Futures.toCompletionStage(sessionsFutureStub.deleteSession(toDeleteSessionRequest(sessionInfo.id())))
                    .thenApply(response -> toSessionState(response.getSession()));
    });
  }



  private Function<ManagedChannel, CompletionStage<BlobInfo>> createBlobInfo(BlobDefinition blobDefinition) {
    return channel -> {
      var request = toResultMetaDataRequest(sessionInfo.id(), List.of(blobDefinition));
      var resultsFutureStub = ResultsGrpc.newFutureStub(channel);
      return Futures.toCompletionStage(resultsFutureStub.createResultsMetaData(request))
                    .thenApply(response -> {
                      var resultRaw = response.getResults(0);
                      return new BlobInfo(
                        BlobId.from(resultRaw.getResultId()),
                        sessionInfo.id(),
                        resultRaw.getName(),
                        resultRaw.getManualDeletion(),
                        resultRaw.getCreatedBy().isBlank() ? null : TaskId.from(resultRaw.getCreatedBy()),
                        Instant.ofEpochSecond(resultRaw.getCreatedAt().getSeconds(), resultRaw.getCreatedAt().getNanos())
                      );
                    });
    };
  }
}
