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
import fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc;
import fr.aneo.armonik.worker.definition.blob.BlobDefinition;
import fr.aneo.armonik.worker.definition.blob.InMemoryBlob;
import fr.aneo.armonik.worker.internal.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.*;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toMap;

/**
 * Service responsible for creating blobs in ArmoniK's object storage.
 * <p>
 * This service handles two upload strategies:
 * </p>
 * <ul>
 *   <li><strong>Inline upload</strong>: For small blobs (≤ 4 MB), data is sent
 *       directly via RPC</li>
 *   <li><strong>File-based upload</strong>: For large blobs (> 4 MB) or streams,
 *       data is written to shared folder and the Agent is notified</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe. Multiple blob creations can be performed concurrently.
 * </p>
 *
 * @see BlobDefinition
 * @see BlobHandle
 * @see BlobFileWriter
 */
public class BlobService {
  private static final Logger logger = LoggerFactory.getLogger(BlobService.class);

  static final int MAX_UPLOAD_SIZE = 4 * 1024 * 1024;

  private final AgentFutureStub agentFutureStub;
  private final BlobFileWriter blobFileWriter;
  private final String communicationToken;
  private final SessionId sessionId;


  public BlobService(AgentGrpc.AgentFutureStub agentFutureStub, BlobFileWriter blobFileWriter, String communicationToken, SessionId sessionId) {
    this.agentFutureStub = requireNonNull(agentFutureStub, "agentFutureStub cannot be null");
    this.blobFileWriter = requireNonNull(blobFileWriter, "blobFileWriter cannot be null");
    this.communicationToken = requireNonNull(communicationToken, "communicationToken cannot be null");
    this.sessionId = requireNonNull(sessionId, "sessionId cannot be null");
  }

  /**
   * Creates multiple blobs from their definitions.
   * <p>
   * This method automatically partitions blobs into two groups:
   * </p>
   * <ul>
   *   <li>Small in-memory blobs (≤ 4 MB) are uploaded inline via {@code CreateResults}</li>
   *   <li>Large blobs or streams are written to the shared folder</li>
   * </ul>
   *
   * @param blobDefinitions map of logical names to blob definitions
   * @return map of logical names to blob handles
   */
  public Map<String, BlobHandle> createBlobs(Map<String, BlobDefinition> blobDefinitions) {
    logger.debug("Creating {} blobs", blobDefinitions.size());
    var partitioned = blobDefinitions.entrySet()
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

    logger.info("Created {} blobs: {} uploaded inline, {} via file", result.size(), uploadableBlobs.size(), fileBasedBlobs.size());

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
  private Map<String, BlobHandle> uploadBlobs(Map<String, BlobDefinition> uploadableBlobs) {
    return uploadableBlobs.entrySet()
                          .stream()
                          .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> uploadBlob((InMemoryBlob) entry.getValue())
                          ));
  }

  /**
   * Writes large blobs or streams to the shared folder via file-based upload.
   */
  private Map<String, BlobHandle> writeBlobsToFile(Map<String, BlobDefinition> fileBasedBlobs) {
    return fileBasedBlobs.entrySet()
                         .stream()
                         .collect(Collectors.toMap(
                           Map.Entry::getKey,
                           entry -> writeBlob(entry.getValue())
                         ));
  }

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


  private BlobHandle writeBlob(BlobDefinition blobDefinition) {
    var request = CreateResultsMetaDataRequest.newBuilder()
                                              .setCommunicationToken(communicationToken)
                                              .setSessionId(sessionId.asString())
                                              .addResults(CreateResultsMetaDataRequest.ResultCreate.newBuilder()
                                                                                                   .setName(blobDefinition.name())
                                                                                                   .build())
                                              .build();

    var deferredBlobInfo = Futures.toCompletionStage(agentFutureStub.createResultsMetaData(request))
                                  .thenApply(response -> toBlobInfo(response.getResults(0)))
                                  .thenApply(blobInfo -> {
                                    blobFileWriter.write(blobInfo.id(), blobDefinition.asStream());
                                    return blobInfo;
                                  });

    return new BlobHandle(sessionId, blobDefinition.name(), deferredBlobInfo);
  }

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
