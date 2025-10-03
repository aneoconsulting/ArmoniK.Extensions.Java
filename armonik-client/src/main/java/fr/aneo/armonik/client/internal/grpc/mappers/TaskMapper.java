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
package fr.aneo.armonik.client.internal.grpc.mappers;

import com.google.protobuf.Duration;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest.TaskCreation;
import fr.aneo.armonik.client.model.BlobId;
import fr.aneo.armonik.client.model.SessionId;
import fr.aneo.armonik.client.model.TaskConfiguration;

import java.util.List;

import static fr.aneo.armonik.api.grpc.v1.Objects.TaskOptions;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest;

public final class TaskMapper {

  public static TaskCreation toTaskCreation(List<BlobId> inputBlobIds, List<BlobId> outputBlobIds, BlobId payloadId, TaskConfiguration taskConfiguration) {
    var builder = TaskCreation.newBuilder()
                              .setPayloadId(payloadId.asString())
                              .addAllDataDependencies(inputBlobIds.stream().map(BlobId::asString).toList())
                              .addAllExpectedOutputKeys(outputBlobIds.stream().map(BlobId::asString).toList());
    if (taskConfiguration != null) {
      builder.setTaskOptions(toTaskOptions(taskConfiguration));
    }

    return builder.build();
  }

  public static SubmitTasksRequest toSubmitTasksRequest(SessionId sessionId, TaskConfiguration taskConfig, TaskCreation taskCreation) {
    return SubmitTasksRequest.newBuilder()
                             .setSessionId(sessionId.asString())
                             .setTaskOptions(toTaskOptions(taskConfig))
                             .addTaskCreations(taskCreation)
                             .build();
  }

  public static TaskOptions toTaskOptions(TaskConfiguration taskConfiguration) {
    return TaskOptions.newBuilder()
                      .setMaxDuration(Duration.newBuilder()
                                              .setSeconds(taskConfiguration.maxDuration().getSeconds())
                                              .setNanos(taskConfiguration.maxDuration().getNano()))
                      .setPriority(taskConfiguration.priority())
                      .setMaxRetries(taskConfiguration.maxRetries())
                      .setPartitionId(taskConfiguration.partitionId())
                      .putAllOptions(taskConfiguration.options())
                      .build();
  }
}
