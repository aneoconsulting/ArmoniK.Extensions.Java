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
package fr.aneo.armonik.worker;

import com.google.protobuf.UnsafeByteOperations;
import fr.aneo.armonik.worker.definition.blob.BlobDefinition;
import fr.aneo.armonik.worker.definition.blob.InMemoryBlob;
import fr.aneo.armonik.worker.definition.blob.InputBlobDefinition;
import fr.aneo.armonik.worker.definition.blob.OutputBlobDefinition;
import fr.aneo.armonik.worker.internal.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.*;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toMap;

/**
 * Service responsible for creating blobs in ArmoniK's object storage.
 * <p>
 * This service handles blob creation in two modes:
 * </p>
 * <ul>
 *   <li><strong>Input blobs with data</strong>: Create blobs and upload data immediately</li>
 *   <li><strong>Blob metadata preparation</strong>: Create blob metadata, provide data later</li>
 * </ul>
 *
 * <h2>Upload Strategies for Input Blobs (createBlobs)</h2>
 * <ul>
 *   <li><strong>Inline upload</strong>: For small blobs (≤ 4 MB), data is sent directly via RPC</li>
 *   <li><strong>File-based upload</strong>: For large blobs (> 4 MB) or streams, data is written to shared folder</li>
 * </ul>
 *
 * <h2>Metadata Preparation (prepareBlobs)</h2>
 * <p>
 * Use {@link #prepareBlobs(Map)} when you need to:
 * </p>
 * <ul>
 *   <li>Create output blob metadata (data will be provided by task execution)</li>
 *   <li>Create input blob metadata (data will be written to files later)</li>
 *   <li>Enable task graph construction before all data is available</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe. Multiple blob operations can be performed concurrently.
 * </p>
 *
 * @see InputBlobDefinition
 * @see OutputBlobDefinition
 * @see BlobHandle
 * @see BlobFileWriter
 */
final class BlobService {
  private static final Logger logger = LoggerFactory.getLogger(BlobService.class);

  static final int MAX_UPLOAD_SIZE = 4 * 1024 * 1024;

  private final AgentFutureStub agentFutureStub;
  private final BlobFileWriter blobFileWriter;
  private final String communicationToken;
  private final SessionId sessionId;

  /**
   * Creates a new blob service for managing blobs within a session.
   *
   * @param agentFutureStub    the gRPC stub for communicating with the Agent
   * @param blobFileWriter     the writer for writing blob data to shared folder
   * @param sessionId          the session identifier
   * @param communicationToken the communication token for this execution context
   * @throws NullPointerException if any parameter is null
   */
  BlobService(AgentFutureStub agentFutureStub, BlobFileWriter blobFileWriter, SessionId sessionId, String communicationToken) {
    this.agentFutureStub = requireNonNull(agentFutureStub, "agentFutureStub cannot be null");
    this.blobFileWriter = requireNonNull(blobFileWriter, "blobFileWriter cannot be null");
    this.communicationToken = requireNonNull(communicationToken, "communicationToken cannot be null");
    this.sessionId = requireNonNull(sessionId, "sessionId cannot be null");
  }

  /**
   * Creates multiple input blobs with their data from input blob definitions.
   * <p>
   * This method automatically partitions blobs into two groups:
   * </p>
   * <ul>
   *   <li>Small in-memory blobs (≤ 4 MB) are uploaded inline via {@code CreateResults}</li>
   *   <li>Large blobs or streams are written to the shared folder via {@code CreateResultsMetaData} + file write</li>
   * </ul>
   *
   * @param inputBlobDefinitions map of logical names to input blob definitions
   * @return map of logical names to blob handles
   * @throws NullPointerException if inputBlobDefinitions is null
   * @see InputBlobDefinition
   * @see #prepareBlobs(Map)
   */
  Map<String, BlobHandle> createBlobs(Map<String, InputBlobDefinition> inputBlobDefinitions) {
    requireNonNull(inputBlobDefinitions, "inputBlobDefinitions cannot be null");

    logger.debug("Creating {} input blobs", inputBlobDefinitions.size());
    var partitioned = inputBlobDefinitions.entrySet()
                                          .stream()
                                          .collect(partitioningBy(
                                            entry -> isUploadable(entry.getValue()),
                                            toMap(Map.Entry::getKey, Map.Entry::getValue)
                                          ));

    var uploadableBlobs = partitioned.get(true);
    var fileBasedBlobs = partitioned.get(false);
    logger.debug("Blob distribution: {} uploadable, {} file-based", uploadableBlobs.size(), fileBasedBlobs.size());

    var result = new HashMap<String, BlobHandle>();
    result.putAll(uploadBlobs(uploadableBlobs));
    result.putAll(writeBlobsToFile(fileBasedBlobs));

    logger.info("Created {} input blobs: {} uploaded inline, {} via file", result.size(), uploadableBlobs.size(), fileBasedBlobs.size());

    return result;
  }

  /**
   * Creates a single input blob with its data from an input blob definition.
   * <p>
   * This method automatically determines the upload strategy based on blob characteristics:
   * </p>
   * <ul>
   *   <li>Small in-memory blobs (≤ 4 MB) are uploaded inline via {@code CreateResults}</li>
   *   <li>Large blobs or streams are written to the shared folder via {@code CreateResultsMetaData} + file write</li>
   * </ul>
   *
   * @param inputBlobDefinition input blob definition containing the data to upload
   * @return blob handle representing the created blob
   * @throws NullPointerException if inputBlobDefinition is null
   * @see InputBlobDefinition
   * @see #createBlobs(Map)
   * @see #prepareBlobs(Map)
   */
  BlobHandle createBlob(InputBlobDefinition inputBlobDefinition) {
    requireNonNull(inputBlobDefinition, "blobDefinition cannot be null");

    return createBlobs(Map.of("blob", inputBlobDefinition)).get("blob");
  }

  /**
   * Prepares multiple blobs by creating their metadata without uploading data.
   * <p>
   * This method creates blob metadata. The actual data will be provided later, either:
   * </p>
   * <ul>
   *   <li>By task execution (for output blobs)</li>
   *   <li>By writing to files in the shared folder (for input blobs)</li>
   * </ul>
   * <p>
   * Common use cases:
   * </p>
   * <ul>
   *   <li><strong>Output blobs</strong>: Expected outputs that tasks will produce</li>
   *   <li><strong>Input blobs</strong>: Large input data written to files after metadata creation</li>
   *   <li><strong>Delayed upload</strong>: Create metadata early, provide data later</li>
   * </ul>
   * <p>
   * The returned blob handles can be used as data dependencies for downstream tasks,
   * enabling task graph construction before all data is available.
   * </p>
   *
   * @param blobDefinitions map of logical names to blob definitions
   * @return map of logical names to blob handles
   * @throws NullPointerException if blobDefinitions is null
   * @see BlobDefinition
   * @see OutputBlobDefinition
   * @see InputBlobDefinition
   * @see #createBlobs(Map)
   */
  <T extends BlobDefinition> Map<String, BlobHandle> prepareBlobs(Map<String, T> blobDefinitions) {
    requireNonNull(blobDefinitions, "blobDefinitions cannot be null");

    logger.debug("Preparing {} blob metadata", blobDefinitions.size());
    var entries = new ArrayList<>(blobDefinitions.entrySet());
    var metadata = entries.stream()
                          .map(entry -> CreateResultsMetaDataRequest.ResultCreate.newBuilder()
                                                                                 .setName(entry.getValue().name())
                                                                                 .build())
                          .toList();
    var request = CreateResultsMetaDataRequest.newBuilder()
                                              .setCommunicationToken(communicationToken)
                                              .setSessionId(sessionId.asString())
                                              .addAllResults(metadata)
                                              .build();

    var response = Futures.toCompletionStage(agentFutureStub.createResultsMetaData(request));

    var deferredBlobInfos = IntStream.range(0, entries.size())
                                     .mapToObj(i -> response.thenApply(r -> toBlobInfo(r.getResults(i))))
                                     .toList();

    var result = new HashMap<String, BlobHandle>(entries.size());
    IntStream.range(0, entries.size()).forEach(i -> {
      var entry = entries.get(i);
      result.put(entry.getKey(), new BlobHandle(sessionId, entry.getValue().name(), deferredBlobInfos.get(i)));
    });

    logger.info("Prepared {} blob metadata", result.size());
    return result;
  }

  /**
   * Checks if a blob can be uploaded inline (small in-memory blob).
   */
  private boolean isUploadable(BlobDefinition definition) {
    return definition instanceof InMemoryBlob blob && blob.data().length <= MAX_UPLOAD_SIZE;
  }

  /**
   * Uploads small in-memory blobs inline via CreateResults RPC.
   * <p>
   * Casting to {@link InMemoryBlob} is safe because partitioning guarantees type.
   * </p>
   */
  private Map<String, BlobHandle> uploadBlobs(Map<String, InputBlobDefinition> uploadableBlobs) {
    return uploadableBlobs.entrySet()
                          .stream()
                          .collect(toMap(
                            Map.Entry::getKey,
                            entry -> uploadBlob((InMemoryBlob) entry.getValue())
                          ));
  }

  /**
   * Writes large blobs or streams to the shared folder via file-based upload.
   */
  private Map<String, BlobHandle> writeBlobsToFile(Map<String, InputBlobDefinition> fileBasedBlobs) {
    return prepareBlobs(fileBasedBlobs).entrySet()
                                       .stream()
                                       .collect(toMap(
                                         Map.Entry::getKey,
                                         entry -> {
                                           var name = entry.getKey();
                                           var handle = entry.getValue();
                                           var blobInfoAfterWrite = handle.deferredBlobInfo()
                                                                          .thenApply(blobInfo -> {
                                                                            blobFileWriter.write(blobInfo.id(), fileBasedBlobs.get(name).asStream());
                                                                            return blobInfo;
                                                                          });
                                           return new BlobHandle(sessionId, handle.name(), blobInfoAfterWrite);
                                         }
                                       ));
  }

  /**
   * Uploads a small in-memory blob inline via CreateResults RPC.
   */
  private BlobHandle uploadBlob(InMemoryBlob blob) {
    var resultCreate = CreateResultsRequest.ResultCreate.newBuilder()
                                                        .setName(blob.name())
                                                        .setData(UnsafeByteOperations.unsafeWrap(blob.data()))
                                                        .build();
    var request = CreateResultsRequest.newBuilder()
                                      .setCommunicationToken(communicationToken)
                                      .setSessionId(sessionId.asString())
                                      .addResults(resultCreate)
                                      .build();

    var deferredBlobInfo = Futures.toCompletionStage(agentFutureStub.createResults(request))
                                  .thenApply(response -> toBlobInfo(response.getResults(0)));

    return new BlobHandle(sessionId, blob.name(), deferredBlobInfo);
  }

  /**
   * Converts gRPC result metadata to domain blob info.
   */
  private static BlobInfo toBlobInfo(ResultMetaData metaData) {
    return new BlobInfo(
      BlobId.from(metaData.getResultId()),
      BlobStatus.fromStatusCode(metaData.getStatus().getNumber()),
      Instant.ofEpochSecond(
        metaData.getCreatedAt().getSeconds(),
        metaData.getCreatedAt().getNanos()
      ));
  }
}
