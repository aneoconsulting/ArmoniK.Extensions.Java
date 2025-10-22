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

import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static fr.aneo.armonik.worker.internal.PathValidator.resolveWithin;
import static fr.aneo.armonik.worker.internal.PathValidator.validateFile;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Default implementation of {@link TaskContextFactory}.
 * <p>
 * This factory creates {@link TaskContext} instances from Agent requests
 *
 * <h2>Validation</h2>
 * <p>
 * The factory performs several validation checks:
 * </p>
 * <ul>
 *   <li>All file paths must be within the data folder (prevents directory traversal)</li>
 *   <li>All input files must exist and be readable</li>
 *   <li>The payload file must exist and contain valid JSON</li>
 *   <li>The payload JSON must conform to the expected structure</li>
 * </ul>
 *
 * <h2>Error Handling</h2>
 * <p>
 * This factory throws {@link ArmoniKException} if:
 * </p>
 * <ul>
 *   <li>The payload file cannot be read ({@link IOException})</li>
 *   <li>The payload contains invalid JSON ({@link JsonParseException})</li>
 *   <li>The payload JSON structure is incorrect ({@link IllegalArgumentException})</li>
 *   <li>Any input file does not exist or is not readable</li>
 *   <li>Any file path attempts directory traversal</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This factory is stateless and thread-safe. The same instance can be reused across
 * multiple task processing requests.
 * </p>
 *
 * @see TaskContext
 * @see TaskContextFactory
 * @see TaskInput
 * @see TaskOutput
 */
public class DefaultTaskContextFactory implements TaskContextFactory {
  private static final Logger logger = LoggerFactory.getLogger(DefaultTaskContextFactory.class);


  @Override
  public TaskContext create(AgentFutureStub agentStub, ProcessRequest request) {
    requireNonNull(agentStub, "agentStub");
    requireNonNull(request, "request");

    var dataFolderPath = Path.of(request.getDataFolder());
    var agentNotifier = new AgentNotifier(agentStub, SessionId.from(request.getSessionId()), request.getCommunicationToken());
    var blobFileWriter = new BlobFileWriter(dataFolderPath, agentNotifier);
    var blobsMapping = createBlobsMapping(dataFolderPath, request.getPayloadId());
    var inputs = createInputs(blobsMapping, dataFolderPath);
    var outputs = createOutputs(blobsMapping, blobFileWriter);
    return new TaskContext(inputs, outputs);
  }


  private static Map<String, TaskInput> createInputs(BlobsMapping blobsMapping, Path dataFolderPath) {
    return blobsMapping.inputsMapping()
                       .entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, createInputTask(dataFolderPath)));
  }

  private static Function<Map.Entry<String, String>, TaskInput> createInputTask(Path dataFolderPath) {
    return entry -> {
      String logicalName = entry.getKey();
      String blobId = entry.getValue();
      try {
        var inputFilePath = resolveWithin(dataFolderPath, blobId);
        validateFile(inputFilePath);
        long fileSize = Files.size(inputFilePath);
        logger.info("Input task created: logicalName='{}', blobId={}, size={} bytes", logicalName, blobId, fileSize);
        return new TaskInput(BlobId.from(entry.getValue()), logicalName, inputFilePath);

      } catch (Exception e) {
        logger.error("Failed to create input task: logicalName='{}', blobId={}", logicalName, blobId, e);
        throw new ArmoniKException("Failed to create input task for: " + logicalName, e);
      }
    };
  }

  private static Map<String, TaskOutput> createOutputs(BlobsMapping blobsMapping, BlobFileWriter writer) {
    return blobsMapping.outputsMapping()
                       .entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, createOutputTask(writer)));
  }

  private static Function<Map.Entry<String, String>, TaskOutput> createOutputTask(BlobFileWriter writer) {
    return entry -> {
      String logicalName = entry.getKey();
      String blobId = entry.getValue();
      try {
        logger.info("Output task created: logicalName='{}', blobId={}", logicalName, blobId);
        return new TaskOutput(BlobId.from(blobId), logicalName, writer);
      } catch (Exception e) {
        logger.error("Failed to create output task: logicalName='{}', blobId={}", logicalName, blobId, e);
        throw new ArmoniKException("Failed to create output task for: " + entry.getKey(), e);
      }
    };
  }

  private static BlobsMapping createBlobsMapping(Path dataFolderPath, String payloadId) {
    var payload = resolveWithin(dataFolderPath, payloadId);
    validateFile(payload);
    try {
      var payloadData = Files.readString(payload);
      return BlobsMapping.fromJson(payloadData);
    } catch (IOException exception) {
      logger.error("Failed to read payload file: blobId={}, dataFolder={}", payloadId, dataFolderPath, exception);
      throw new ArmoniKException("Failed to read payload file: " + payloadId, exception);
    } catch (JsonParseException exception) {
      logger.error("Payload contains invalid JSON: blobId={}", payloadId, exception);
      throw new ArmoniKException("Payload contains invalid JSON: " + payloadId, exception);
    } catch (IllegalArgumentException exception) {
      logger.error("Payload JSON structure is incorrect: blobId={}, error={}", payloadId, exception.getMessage());
      throw new ArmoniKException("Payload JSON structure is incorrect: " + payloadId, exception);
    }
  }
}
