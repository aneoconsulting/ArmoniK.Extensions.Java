package fr.aneo.armonik.worker;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;
import static java.util.stream.Collectors.toMap;

public class TaskHandler {
  private static final Gson gson = new Gson();
  private final AgentFutureStub agentStub;
  private final Map<String, InputTask> inputs;
  private final Map<String, OutputTask> outputs;

  private TaskHandler(AgentFutureStub agentStub, Map<String, InputTask> inputs, Map<String, OutputTask> outputs) {
    this.agentStub = agentStub;
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
    var dataFolderPath = Path.of(request.getDataFolder());
    var associationTable = getBlobsMapping(dataFolderPath, request.getPayloadId());
    var inputs = getInputs(associationTable, dataFolderPath);
    var outputs = getOutputs(associationTable, dataFolderPath);
    return new TaskHandler(agentStub, inputs, outputs);
  }

  private static Map<String, InputTask> getInputs(BlobsMapping blobsMapping, Path dataFolderPath) {
    return blobsMapping.inputsMapping()
                       .entrySet()
                       .stream()
                       .collect(toMap(
                             Map.Entry::getKey,
                             entry -> {
                               var inputFilePath = resolveWithin(dataFolderPath, entry.getValue());
                               validateFile(inputFilePath);
                               return new InputTask(entry.getKey(), inputFilePath);
                             }));
  }

  private static Map<String, OutputTask> getOutputs(BlobsMapping blobsMapping, Path dataFolderPath) {
    return blobsMapping.outputsMapping()
                       .entrySet()
                       .stream()
                       .collect(toMap(
                             Map.Entry::getKey,
                             entry -> {
                               var outputFilePath = resolveWithin(dataFolderPath, entry.getValue());
                               return new OutputTask(entry.getKey(), outputFilePath);
                             }));
  }

  private static BlobsMapping getBlobsMapping(Path dataFolderPath, String payloadId) {
    var payload = resolveWithin(dataFolderPath, payloadId);
    validateFile(payload);
    try {
      var payloadData = Files.readString(payload);
      return BlobsMapping.fromJson(payloadData);
    } catch (IOException exception) {
      throw new ArmoniKException("Failed to load Payload: " + payloadId + ", dataFolder: " + dataFolderPath, exception);
    } catch (JsonParseException | ClassCastException exception) {
      throw new ArmoniKException("Failed to load Payload. Invalid json " + payloadId + ", dataFolder: " + dataFolderPath, exception);
    }
  }

  private static void validateFile(Path path) {
    if (!Files.exists(path)) {
      throw new ArmoniKException("File " + path + " does not exist");
    }
    if (Files.isDirectory(path)) {
      throw new ArmoniKException("Path " + path + " is a directory");
    }
  }

  private static Path resolveWithin(Path root, String child) {
    var normalizedRoot = root.normalize();
    var resolved = normalizedRoot.resolve(child).normalize();

    if (!resolved.startsWith(normalizedRoot)) {
      throw new ArmoniKException(child + " resolves outside data folder: " + resolved);
    }
    if (!resolved.getParent().equals(normalizedRoot)) {
      throw new ArmoniKException("Only files at the root of dataFolder are allowed: '" + child + "' is not valid");
    }
    return resolved;
  }
}
