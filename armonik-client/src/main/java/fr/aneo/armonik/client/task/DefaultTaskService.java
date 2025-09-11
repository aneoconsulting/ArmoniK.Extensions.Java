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
package fr.aneo.armonik.client.task;

import com.google.protobuf.Duration;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest.TaskCreation;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksGrpc;
import fr.aneo.armonik.client.blob.BlobHandle;
import fr.aneo.armonik.client.blob.BlobMetadata;
import fr.aneo.armonik.client.session.SessionHandle;
import fr.aneo.armonik.client.util.Futures;
import io.grpc.ManagedChannel;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static fr.aneo.armonik.api.grpc.v1.Objects.TaskOptions;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest;
import static fr.aneo.armonik.client.util.Futures.toCompletionStage;
import static java.util.Objects.requireNonNull;

public class DefaultTaskService implements TaskService {
  private final TasksGrpc.TasksFutureStub tasksStub;

  public DefaultTaskService(ManagedChannel channel) {
    tasksStub = TasksGrpc.newFutureStub(channel);
  }

  @Override
  public TaskHandle submitTask(SessionHandle sessionHandle,
                               Map<String, BlobHandle> inputs,
                               Map<String, BlobHandle> outputs,
                               BlobHandle payload,
                               TaskConfiguration taskConfiguration) {
    requireNonNull(sessionHandle, "session must not be null");
    requireNonNull(inputs, "inputs must not be null");
    requireNonNull(outputs, "outputs must not be null");
    requireNonNull(payload, "payload must not be null");

    var inputIdStages = getIdsFrom(inputs.values());
    var outputIdStages = getIdsFrom(outputs.values());

    var responseStage = payload.metadata()
                               .thenApply(BlobMetadata::id)
                               .thenCombine(inputIdStages, AllInputs::new)
                               .thenCombine(outputIdStages, this::toTaskCreation)
                               .thenApply(taskCreation -> toSubmitTasksRequest(sessionHandle, taskConfiguration, taskCreation))
                               .thenCompose(request -> toCompletionStage(tasksStub.submitTasks(request)));

    return new TaskHandle(
      sessionHandle,
      taskConfiguration == null ? sessionHandle.defaultTaskConfiguration() : taskConfiguration,
      responseStage.thenApply(response -> new TaskMetadata(UUID.fromString(response.getTaskInfos(0).getTaskId()))),
      inputs,
      outputs,
      payload
    );
  }

  private SubmitTasksRequest toSubmitTasksRequest(SessionHandle sessionHandle, TaskConfiguration taskConfiguration, TaskCreation taskCreation) {
    var taskConfig = taskConfiguration == null ? sessionHandle.defaultTaskConfiguration() : taskConfiguration;
    return SubmitTasksRequest.newBuilder()
                             .setSessionId(sessionHandle.id().toString())
                             .setTaskOptions(TaskOptions.newBuilder()
                                                        .setMaxDuration(Duration.newBuilder()
                                                                                .setSeconds(taskConfig.maxDuration().getSeconds())
                                                                                .setNanos(taskConfig.maxDuration().getNano()))
                                                        .setPriority(taskConfig.priority())
                                                        .setMaxRetries(taskConfig.maxRetries())
                                                        .setPartitionId(taskConfig.partitionId())
                                                        .putAllOptions(taskConfig.options())
                             )
                             .addTaskCreations(taskCreation)
                             .build();
  }

  private TaskCreation toTaskCreation(AllInputs allInputs, List<UUID> outputIds) {
    return TaskCreation.newBuilder()
                       .setPayloadId(allInputs.payloadId.toString())
                       .addAllDataDependencies(allInputs.inputIds.stream().map(UUID::toString).toList())
                       .addAllExpectedOutputKeys(outputIds.stream().map(UUID::toString).toList())
                       .build();
  }

  private static CompletionStage<List<UUID>> getIdsFrom(Collection<BlobHandle> blobHandles) {
    return Futures.allOf(blobHandles.stream().map(BlobHandle::metadata).toList())
                  .thenApply(blobMetadata -> blobMetadata.stream().map(BlobMetadata::id).toList());
  }

  private record AllInputs(UUID payloadId, List<UUID> inputIds) {
  }
}
