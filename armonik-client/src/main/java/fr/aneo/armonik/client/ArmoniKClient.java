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

import fr.aneo.armonik.client.blob.BlobDefinition;
import fr.aneo.armonik.client.blob.BlobHandle;
import fr.aneo.armonik.client.blob.BlobMetadata;
import fr.aneo.armonik.client.payload.JsonPayloadSerializer;
import fr.aneo.armonik.client.payload.PayloadSerializer;
import fr.aneo.armonik.client.session.SessionHandle;
import fr.aneo.armonik.client.task.TaskConfiguration;
import fr.aneo.armonik.client.task.TaskDefinition;
import fr.aneo.armonik.client.task.TaskHandle;
import fr.aneo.armonik.client.util.Futures;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import fr.aneo.armonik.client.blob.BlobHandlesAllocation;

import static java.util.stream.Collectors.toMap;

/**
 * High-level entry point for interacting with the ArmoniK distributed computing platform.
 * <p>
 * ArmoniK is an open-source, serverless grid orchestrator that distributes compute-intensive
 * tasks across workers. The {@code ArmoniKClient} provides a developer-friendly API for
 * submitting tasks to the ArmoniK cluster and managing their inputs and outputs.
 *
 * <h2>Core Concepts</h2>
 * <ul>
 *   <li><strong>Session:</strong> A logical workspace that scopes task submissions and blob management</li>
 *   <li><strong>Task:</strong> A unit of work with inputs (data blobs) and declared outputs</li>
 *   <li><strong>Blob:</strong> Binary data objects representing task inputs and outputs</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 * <p>Basic task submission:
 * <pre>{@code
 * // Configure and create client
 * ArmoniKClient client = ArmoniKClient.newBuilder()
 *     .withConnectionConfiguration(connectionConfig)
 *     .withPartition("compute-partition")
 *     .build();
 *
 * // Define and submit task
 * TaskDefinition task = new TaskDefinition()
 *     .withInput("data", BlobDefinition.from(inputBytes))
 *     .withOutput("result");
 *
 * TaskHandle handle = client.submitTask(task);
 *
 * // Access outputs when ready
 * CompletionStage<byte[]> result = handle.output("result")
 *     .thenCompose(blobHandle -> client.services().blobs()
 *         .downloadBlob(client.session, blobHandle.metadata()...));
 * }</pre>
 *
 * <p>Advanced usage with services:
 * <pre>{@code
 * // Direct service access for advanced scenarios
 * Services services = client.services();
 * BlobService blobs = services.blobs();
 * TaskService tasks = services.tasks();
 * SessionService sessions = services.sessions();
 * }</pre>
 *
 * @see ArmoniKClientBuilder
 * @see TaskDefinition
 * @see TaskHandle
 * @see SessionHandle
 * @see Services
 * @see fr.aneo.armonik.client.blob.BlobDefinition
 * @see fr.aneo.armonik.client.task.TaskConfiguration
 */
public class ArmoniKClient {
  private final Services services;
  private final PayloadSerializer payloadSerializer;
  public SessionHandle sessionHandle;

  /**
   * Creates a client and opens an ArmoniK {@link SessionHandle}.
   * <p>
   * This constructor is package-private and typically called by {@link ArmoniKClientBuilder}.
   * The client establishes a session within the ArmoniK cluster using the provided configuration.
   * </p>
   *
   * <p>
   * If {@code partitionIds} is {@code null} or empty, the session will use default partitioning.
   * If {@code taskConfiguration} is {@code null}, library defaults are applied for task execution parameters.
   * </p>
   *
   * @param partitionIds      set of partition identifiers to associate with the session;
   *                         may be {@code null} or empty to use default partitioning
   * @param taskConfiguration default task configuration for submitted tasks;
   *                         when {@code null}, library defaults are applied
   * @param payloadSerializer serializer for task payload manifests; must not be {@code null}
   * @param services          the service facade providing access to ArmoniK operations; must not be {@code null}
   * @throws NullPointerException if {@code payloadSerializer} or {@code services} is {@code null}
   * @see ArmoniKClientBuilder#build()
   */
  ArmoniKClient(Set<String> partitionIds,
                TaskConfiguration taskConfiguration,
                PayloadSerializer payloadSerializer,
                Services services) {
    var effectivePartitionIds = partitionIds == null ? Set.<String>of() : partitionIds;
    var effectiveTaskConfiguration = taskConfiguration == null ? TaskConfiguration.defaultConfiguration() : taskConfiguration;
    this.payloadSerializer = payloadSerializer;
    this.services = services;
    this.sessionHandle = services.sessions().createSession(effectivePartitionIds, effectiveTaskConfiguration);
  }

  ArmoniKClient(Set<String> partitionIds,
                TaskConfiguration taskConfiguration,
                Services services) {
    this(partitionIds, taskConfiguration, new JsonPayloadSerializer(), services);
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
   * <p>
   * This method orchestrates the complete task submission workflow:
   * <ol>
   *   <li>Allocates blob handles for inputs and outputs</li>
   *   <li>Uploads inline input data to the ArmoniK cluster</li>
   *   <li>Creates and uploads the task payload manifest</li>
   *   <li>Submits the task for execution by workers</li>
   * </ol>
   *
   * <p>
   * The task will be queued for execution on available workers within the configured partitions.
   * Use the returned {@link TaskHandle} to monitor task progress and access outputs when ready.
   * </p>
   *
   * <p>
   * <strong>Note:</strong> This method handles both {@link fr.aneo.armonik.client.blob.BlobDefinition}
   * (inline data) and existing {@link fr.aneo.armonik.client.blob.BlobHandle} inputs seamlessly.
   * </p>
   *
   * @param taskDefinition the task definition specifying inputs, outputs, and execution options;
   *                      must not be {@code null}
   * @return a handle to the submitted task for monitoring and output access; never {@code null}
   * @throws NullPointerException if {@code taskDefinition} is {@code null}
   * @throws RuntimeException if task submission fails due to network, serialization, or server errors
   * @see TaskDefinition
   * @see TaskHandle
   */
  public TaskHandle submitTask(TaskDefinition taskDefinition) {
    Objects.requireNonNull(taskDefinition, "taskDefinition must not be null");

    var allocation = services.blobs().allocateBlobHandles(sessionHandle, taskDefinition);
    var blobDefinitionByHandle = allocation.inputHandlesByName().entrySet()
                                           .stream()
                                           .collect(toMap(
                                             Map.Entry::getValue,
                                             entry -> taskDefinition.inputDefinitions().get(entry.getKey())));

    uploadInputs(blobDefinitionByHandle);

    var allInputHandles = Stream.concat(allocation.inputHandlesByName().entrySet().stream(), taskDefinition.inputHandles().entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    var inputIdStages = allInputHandles.entrySet().stream().collect(ids());
    var outputIdStages = allocation.outputHandlesByName().entrySet().stream().collect(ids());

    Futures.allOf(inputIdStages)
           .thenCombine(Futures.allOf(outputIdStages), payloadSerializer::serialize)
           .thenCompose(uploadPayload(allocation));

    return services.tasks()
                   .submitTask(
                     sessionHandle,
                     allInputHandles,
                     allocation.outputHandlesByName(),
                     allocation.payloadHandle(),
                     taskDefinition.configuration());
  }

  /**
   * Returns a builder to configure and create a new {@link ArmoniKClient}.
   *
   * @return a new builder
   */
  public static ArmoniKClientBuilder newBuilder() {
    return new ArmoniKClientBuilder();
  }

  private static Collector<Map.Entry<String, BlobHandle>, ?, Map<String, CompletionStage<UUID>>> ids() {
    return toMap(
      Map.Entry::getKey,
      entry -> entry.getValue().metadata().thenApply(BlobMetadata::id)
    );
  }

  private Function<BlobDefinition, CompletionStage<Void>> uploadPayload(BlobHandlesAllocation allocation) {
    return payloadDefinition -> services.blobs().uploadBlobData(allocation.payloadHandle(), payloadDefinition);
  }

  private void uploadInputs(Map<BlobHandle, BlobDefinition> blobHandleDefinitions) {
    blobHandleDefinitions.forEach((handle, definition) -> services.blobs().uploadBlobData(handle, definition));
  }
}
