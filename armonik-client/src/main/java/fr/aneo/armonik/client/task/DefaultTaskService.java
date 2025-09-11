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
import fr.aneo.armonik.api.grpc.v1.Objects;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest.TaskCreation;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksGrpc;
import fr.aneo.armonik.client.blob.BlobHandle;
import fr.aneo.armonik.client.blob.BlobMetadata;
import fr.aneo.armonik.client.session.SessionHandle;
import io.grpc.ManagedChannel;

import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest;
import static fr.aneo.armonik.client.util.FutureAdapters.toCompletionStage;
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
                               TaskConfiguration taskConfiguration) {
    requireNonNull(sessionHandle, "session must not be null");
    requireNonNull(inputs, "inputs must not be null");
    requireNonNull(outputs, "outputs must not be null");

    var outputIdStage = outputs.entrySet().iterator().next().getValue().metadata().thenApply(BlobMetadata::id);
    var inputIdStage = inputs.entrySet().iterator().next().getValue().metadata().thenApply(BlobMetadata::id);
    var responseStage = inputIdStage.thenCombine(outputIdStage, toBuildRequest(sessionHandle, taskConfiguration))
                               .thenCompose(request -> toCompletionStage(tasksStub.submitTasks(request)));

    return new TaskHandle(
      sessionHandle,
      taskConfiguration == null ? sessionHandle.defaultTaskConfiguration() : taskConfiguration,
      responseStage.thenApply(response -> new TaskMetadata(UUID.fromString(response.getTaskInfos(0).getTaskId()))),
      inputs,
      outputs,
      inputs.entrySet().iterator().next().getValue()
    );
  }

  private static BiFunction<UUID, UUID, SubmitTasksRequest> toBuildRequest(SessionHandle sessionHandle, TaskConfiguration taskConfiguration) {
    var taskConfig = taskConfiguration == null ? sessionHandle.defaultTaskConfiguration() : taskConfiguration;
    var taskOptionsBuilder = Objects.TaskOptions.newBuilder()
                                                .setMaxDuration(Duration.newBuilder()
                                                                        .setSeconds(taskConfig.maxDuration().getSeconds())
                                                                        .setNanos(taskConfig.maxDuration().getNano()))
                                                .setPriority(taskConfig.priority())
                                                .setMaxRetries(taskConfig.maxRetries())
                                                .setPartitionId(taskConfig.partitionId());

    return (inputId, outputId) -> SubmitTasksRequest.newBuilder()
                                                    .setSessionId(sessionHandle.id().toString())
                                                    .setTaskOptions(taskOptionsBuilder)
                                                    .addTaskCreations(TaskCreation.newBuilder()
                                                                                  .setPayloadId(inputId.toString())
                                                                                  .addExpectedOutputKeys(outputId.toString()))
                                                    .build();
  }
}
