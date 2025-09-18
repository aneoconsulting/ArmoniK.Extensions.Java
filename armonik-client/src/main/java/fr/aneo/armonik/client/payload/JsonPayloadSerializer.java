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
package fr.aneo.armonik.client.payload;

import com.google.gson.Gson;
import fr.aneo.armonik.client.blob.BlobDefinition;

import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Map.Entry;
import static java.util.stream.Collectors.toMap;

public class JsonPayloadSerializer implements PayloadSerializer {

  private final Gson gson = new Gson();

  @Override
  public BlobDefinition serialize(Map<String, UUID> inputIds, Map<String, UUID> outputIds) {
    var in  = inputIds.entrySet()
                      .stream()
                      .collect(toMap(Entry::getKey, e -> e.getValue().toString()));
    var out = outputIds.entrySet()
                       .stream()
                       .collect(toMap(Entry::getKey, e -> e.getValue().toString()));

    var payload = Map.of("inputs", in, "outputs", out);
    byte[] bytes = gson.toJson(payload).getBytes(UTF_8);

    return BlobDefinition.from(bytes);
  }
}
