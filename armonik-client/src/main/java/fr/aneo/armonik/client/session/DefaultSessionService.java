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
package fr.aneo.armonik.client.session;

/*
 * This file is part of the ArmoniK project
 *
 * Copyright (C) ANEO, 2025-2025. All rights reserved.
 *   C. Amory          <camory@ext.aneo.fr>
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.protobuf.Duration;
import fr.aneo.armonik.api.grpc.v1.sessions.SessionsGrpc;
import fr.aneo.armonik.client.task.TaskConfiguration;
import io.grpc.ManagedChannel;

import java.util.Set;
import java.util.UUID;

import static fr.aneo.armonik.api.grpc.v1.Objects.TaskOptions;
import static fr.aneo.armonik.api.grpc.v1.sessions.SessionsCommon.CreateSessionRequest;
import static fr.aneo.armonik.client.task.TaskConfiguration.defaultConfiguration;

public class DefaultSessionService implements SessionService {

  private final ManagedChannel channel;

  public DefaultSessionService(ManagedChannel channel) {
    this.channel = channel;
  }

  @Override
  public SessionHandle createSession(Set<String> partitionIds) {
    return createSession(partitionIds, null);
  }

  @Override
  public SessionHandle createSession(Set<String> partitionIds, TaskConfiguration taskConfiguration) {
    var taskConfig = taskConfiguration == null ? defaultConfiguration() : taskConfiguration;

    validateTaskDefaultPartition(partitionIds, taskConfig);

    var createSessionRequest = CreateSessionRequest
      .newBuilder()
      .addAllPartitionIds(partitionIds)
      .setDefaultTaskOption(TaskOptions.newBuilder()
                                       .setMaxDuration(Duration.newBuilder()
                                                               .setSeconds(taskConfig.maxDuration().getSeconds())
                                                               .setNanos(taskConfig.maxDuration().getNano()))
                                       .setPriority(taskConfig.priority())
                                       .setMaxRetries(taskConfig.maxRetries())
                                       .setPartitionId(taskConfig.partitionId()))
      .build();

    var sessionReply = SessionsGrpc.newBlockingStub(channel).createSession(createSessionRequest);

    return new SessionHandle(
      UUID.fromString(sessionReply.getSessionId()),
      partitionIds,
      taskConfig
    );
  }

  private void validateTaskDefaultPartition(Set<String> sessionPartitions, TaskConfiguration taskConfiguration) {
    if (!sessionPartitions.isEmpty() &&
      taskConfiguration.hasExplicitPartition() &&
      !sessionPartitions.contains(taskConfiguration.partitionId())) {
      throw new IllegalArgumentException(
        "TaskConfiguration.partitionId (" + taskConfiguration.partitionId() + ") must be one of client's partitionIds " + sessionPartitions);
    }
  }
}
