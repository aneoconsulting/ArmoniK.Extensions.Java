package fr.aneo.armonik.worker;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class BlobsMapping {
  private static final Gson gson = new Gson();

  private final Map<String, Map<String, String>> mapping;

  private BlobsMapping(Map<String, Map<String, String>> mapping) {
    this.mapping = mapping;
  }

  Map<String, String> inputsMapping() {
    return Map.copyOf(mapping.get("inputs"));
  }
  Map<String, String> outputsMapping() {
    return Map.copyOf(mapping.get("outputs"));
  }

  static BlobsMapping fromJson(String jsonString) {
    requireNonNull(jsonString, "jsonString cannot be null");

    var type = new TypeToken<Map<String, Map<String, String>>>() {
    }.getType();
    Map<String, Map<String, String>> mapping = gson.fromJson(jsonString, type);

    return new BlobsMapping(mapping);
  }
}
