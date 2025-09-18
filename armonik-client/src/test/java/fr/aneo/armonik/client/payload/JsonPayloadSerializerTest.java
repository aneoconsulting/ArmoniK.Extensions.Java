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
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class JsonPayloadSerializerTest {

  private final Gson gson = new Gson();
  private final JsonPayloadSerializer serializer = new JsonPayloadSerializer();

  @Test
  @SuppressWarnings("unchecked")
  void should_serialize_payload_content_in_json_format() {
    // Given
    var inputIds = Map.of(
      "name", UUID.fromString("11111111-1111-1111-1111-111111111111"),
      "age", UUID.fromString("22222222-2222-2222-2222-222222222222"
      ));
    var outputIds = Map.of(
      "result", UUID.fromString("33333333-3333-3333-3333-333333333333"
      ));

    // When
    BlobDefinition blobDefinition = serializer.serialize(inputIds, outputIds);

    // Then
    String json = new String(blobDefinition.data(), UTF_8);
    Map<String, Map<String, String>> root = gson.fromJson(json, Map.class);
    assertThat(root.get("inputs"))
      .containsEntry("name", "11111111-1111-1111-1111-111111111111")
      .containsEntry("age", "22222222-2222-2222-2222-222222222222")
      .hasSize(2);

    assertThat(root.get("outputs"))
      .containsEntry("result", "33333333-3333-3333-3333-333333333333")
      .hasSize(1);
  }
}
