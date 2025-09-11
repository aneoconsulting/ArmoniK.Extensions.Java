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

import fr.aneo.armonik.client.session.SessionHandle;
import fr.aneo.armonik.client.task.TaskConfiguration;
import fr.aneo.armonik.client.task.TaskDefinition;
import fr.aneo.armonik.client.task.TaskHandle;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

/**
 * High-level entry point for interacting with ArmoniK.
 * <p>
 * An {@code ArmoniKClient} maintains a {@link SessionHandle} that scopes task submissions
 * and blob management, and exposes a {@link #services()} facade for advanced scenarios.
 * </p>
 * <p>
 * Use {@link #newBuilder()} to configure and construct a client.
 * </p>
 */
public class ArmoniKClient {

  private final Services services;
  public SessionHandle sessionHandle;

  /**
   * Creates a client and opens an ArmoniK {@link SessionHandle}.
   * <p>
   * If {@code partitionIds} or {@code taskConfiguration} are {@code null}, sensible defaults
   * are applied.
   * </p>
   *
   * @param partitionIds      set of partition identifiers to associate with the session; may be {@code null} or empty
   * @param taskConfiguration default task configuration; when {@code null}, a default configuration is used
   * @param services          the service facade container used by this client
   */
  ArmoniKClient(Set<String> partitionIds, TaskConfiguration taskConfiguration, Services services) {
    var effectivePartitionIds = partitionIds == null ? Set.<String>of() : partitionIds;
    var effectiveTaskConfiguration = taskConfiguration == null ? TaskConfiguration.defaultConfiguration() : taskConfiguration;
    this.services = services;
    this.sessionHandle = services.sessions().createSession(effectivePartitionIds, effectiveTaskConfiguration);
  }

  /**
   * Returns the Services facade for advanced scenarios.
   * <p>
   * The facade provides access to session, blob, and task services for use cases not directly
   * covered by {@code ArmoniKClient}'s convenience methods.
   * </p>
   *
   * @return the Services facade
   */
  public Services services() {
    return services;
  }

  /**
   * Submits a task described by the given {@link TaskDefinition} within the current session.
   *
   * @param taskDefinition the task definition (inputs, outputs, and options)
   * @return a handle to the submitted task
   * @throws NullPointerException if {@code taskDefinition} is {@code null}
   */
  public TaskHandle submitTask(TaskDefinition taskDefinition) {
    var outputs = services.blobs().createBlobMetaData(sessionHandle, taskDefinition.outputs().size());
    var outputHandles = IntStream.range(0, taskDefinition.outputs().size())
                                 .boxed()
                                 .collect(toMap(
                                   index -> taskDefinition.outputs().get(index),
                                   outputs::get
                                 ));

    var inputDefinitions = new ArrayList<>(taskDefinition.inputs().entrySet());
    var inputs = services.blobs().createBlobs(sessionHandle, inputDefinitions.stream().map(Map.Entry::getValue).toList());
    var inputHandles = IntStream.range(0, inputDefinitions.size())
                                .boxed()
                                .collect(toMap(
                                  index -> inputDefinitions.get(index).getKey(),
                                  inputs::get
                                ));

    return services.tasks().submitTask(sessionHandle, inputHandles, outputHandles, taskDefinition.configuration());
  }

  /**
   * Returns a builder to configure and create a new {@link ArmoniKClient}.
   *
   * @return a new builder
   */
  public static ArmoniKClientBuilder newBuilder() {
    return new ArmoniKClientBuilder();
  }
}
