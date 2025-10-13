package fr.aneo.armonik.worker;

import com.google.gson.JsonParseException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static fr.aneo.armonik.worker.BlobListener.AgentNotifier;
import static fr.aneo.armonik.worker.internal.PathValidator.resolveWithin;
import static fr.aneo.armonik.worker.internal.PathValidator.validateFile;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class TaskHandler {
  private final Map<String, InputTask> inputs;
  private final Map<String, OutputTask> outputs;

  private TaskHandler(Map<String, InputTask> inputs, Map<String, OutputTask> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public Map<String, InputTask> inputs() {
    return Map.copyOf(inputs);
  }

  public boolean hasInput(String name) {
    return inputs.containsKey(name);
  }

  public InputTask getInput(String name) {
    if (name == null || name.isEmpty()) throw new IllegalArgumentException("Input name cannot be null or empty");
    if (!hasInput(name)) throw new IllegalArgumentException("Input with name '" + name + "' not found");

    return inputs.get(name);
  }

  public boolean hasOutput(String name) {
    return outputs.containsKey(name);
  }

  public OutputTask getOutput(String name) {
    if (name == null || name.isEmpty()) throw new IllegalArgumentException("Output name cannot be null or empty");
    if (!hasOutput(name)) throw new IllegalArgumentException("Output with name '" + name + "' not found");

    return outputs.get(name);
  }

  public Map<String, OutputTask> outputs() {
    return Map.copyOf(outputs);
  }

  static TaskHandler from(AgentFutureStub agentStub, ProcessRequest request) {
    requireNonNull(agentStub, "agentStub");
    requireNonNull(request, "request");

    var dataFolderPath = Path.of(request.getDataFolder());
    var blobsMapping = createBlobsMapping(dataFolderPath, request.getPayloadId());
    var inputs = createInputs(blobsMapping, dataFolderPath);
    var outputs = createOutputs(blobsMapping, dataFolderPath, new AgentNotifier(agentStub, request.getSessionId(), request.getCommunicationToken()));
    return new TaskHandler(inputs, outputs);
  }

  private static Map<String, InputTask> createInputs(BlobsMapping blobsMapping, Path dataFolderPath) {
    return blobsMapping.inputsMapping()
                       .entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, createInputTask(dataFolderPath)));
  }

  private static Function<Map.Entry<String, String>, InputTask> createInputTask(Path dataFolderPath) {
    return entry -> {
      try {
        var inputFilePath = resolveWithin(dataFolderPath, entry.getValue());
        validateFile(inputFilePath);
        return new InputTask(BlobId.from(entry.getValue()), entry.getKey(), inputFilePath);
      } catch (Exception e) {
        throw new ArmoniKException("Failed to create input task for: " + entry.getKey(), e);
      }
    };
  }

  private static Map<String, OutputTask> createOutputs(BlobsMapping blobsMapping, Path dataFolderPath, BlobListener listener) {
    return blobsMapping.outputsMapping()
                       .entrySet()
                       .stream()
                       .collect(toMap(Map.Entry::getKey, createOutputTask(dataFolderPath, listener)));
  }

  private static Function<Map.Entry<String, String>, OutputTask> createOutputTask(Path dataFolderPath, BlobListener listener) {
    return entry -> {
      try {
        return new OutputTask(
          BlobId.from(entry.getValue()),
          entry.getKey(),
          resolveWithin(dataFolderPath, entry.getValue()),
          listener
        );
      } catch (Exception e) {
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
      throw new ArmoniKException("Failed to read payload file: " + payloadId, exception);
    } catch (JsonParseException exception) {
      throw new ArmoniKException("Payload contains invalid JSON: " + payloadId, exception);
    } catch (IllegalArgumentException exception) {
      throw new ArmoniKException("Payload JSON structure is incorrect: " + payloadId, exception);
    }
  }
}
