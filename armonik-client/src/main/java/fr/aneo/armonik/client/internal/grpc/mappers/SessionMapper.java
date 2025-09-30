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

import fr.aneo.armonik.client.definition.SessionDefinition;

import static fr.aneo.armonik.api.grpc.v1.sessions.SessionsCommon.CreateSessionRequest;
import static fr.aneo.armonik.client.internal.grpc.mappers.TaskMapper.toTaskOptions;

public final class SessionMapper {

  private SessionMapper() {
  }

  public static CreateSessionRequest toCreateSessionRequest(SessionDefinition sessionDefinition) {
    return CreateSessionRequest.newBuilder()
                               .addAllPartitionIds(sessionDefinition.partitionIds())
                               .setDefaultTaskOption(toTaskOptions(sessionDefinition.taskConfiguration()))
                               .build();
  }
}
