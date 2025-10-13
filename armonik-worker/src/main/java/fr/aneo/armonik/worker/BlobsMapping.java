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

    validateMapping(mapping);

    return new BlobsMapping(mapping);
  }

  private static void validateMapping(Map<String, Map<String, String>> mapping) {
    if (mapping == null) {
      throw new IllegalArgumentException("BlobsMapping JSON cannot be null or empty");
    }

    if (!mapping.containsKey("inputs")) {
      throw new IllegalArgumentException("BlobsMapping JSON must contain 'inputs' key");
    }

    if (!mapping.containsKey("outputs")) {
      throw new IllegalArgumentException("BlobsMapping JSON must contain 'outputs' key");
    }

    if (mapping.get("inputs") == null) {
      throw new IllegalArgumentException("BlobsMapping 'inputs' value cannot be null");
    }

    if (mapping.get("outputs") == null) {
      throw new IllegalArgumentException("BlobsMapping 'outputs' value cannot be null");
    }
  }
}
