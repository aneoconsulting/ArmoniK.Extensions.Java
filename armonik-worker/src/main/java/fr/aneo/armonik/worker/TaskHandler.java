package fr.aneo.armonik.worker;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

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

  private TaskHandler(AgentFutureStub agentStub, Map<String, InputTask> inputs) {
    this.agentStub = agentStub;
    this.inputs = inputs;
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

  static TaskHandler from(AgentFutureStub agentStub, ProcessRequest request) {
    var dataFolderPath = Path.of(request.getDataFolder());
    var associationTable = loadAssociationTable(dataFolderPath, request.getPayloadId());
    var inputs = getInputs(associationTable, dataFolderPath);
    return new TaskHandler(agentStub, inputs);
  }

  private static Map<String, InputTask> getInputs(Map<String, Map<String, String>> associationTable, Path dataFolderPath) {
    return associationTable.get("inputs")
                           .entrySet()
                           .stream()
                           .collect(toMap(
                             Map.Entry::getKey,
                             entry -> {
                               var inputFilePath = resolveWithin(dataFolderPath, entry.getValue());
                               validateFile(inputFilePath);
                               return new InputTask(inputFilePath, entry.getKey());
                             }));
  }

  private static Map<String, Map<String, String>> loadAssociationTable(Path dataFolderPath, String payloadId) {
    var payload = resolveWithin(dataFolderPath, payloadId);
    validateFile(payload);

    try {
      var payloadData = Files.readAllBytes(payload);
      var type = new TypeToken<Map<String, Map<String, String>>>() {
      }.getType();

      return gson.fromJson(new String(payloadData), type);
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
    return resolved;
  }
}
