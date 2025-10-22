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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class TaskContextTest {

  @TempDir
  private Path tempDir;
  private BlobFileWriter blobFileWriter;

  @BeforeEach
  void setUp() {
    blobFileWriter = mock(BlobFileWriter.class);
  }
  @Test
  @DisplayName("Should throw an exception when input name is missing")
  void should_throw_exception_when_input_name_is_missing() {
    // Given
    var taskContext = new TaskContext(
      Map.of("name", new TaskInput(BlobId.from("name-id"), "name", tempDir.resolve("name-id"))),
      Map.of("result", new TaskOutput(BlobId.from("result-id"), "age", blobFileWriter))
    );

    // When - Then
    assertThatThrownBy(() -> taskContext.getInput("address")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("Should throw an exception when output name is missing")
  void should_throw_exception_when_output_name_is_missing() {
    // Given
    var taskContext = new TaskContext(
      Map.of("name", new TaskInput(BlobId.from("name-id"), "name", tempDir.resolve("name-id"))),
      Map.of("result", new TaskOutput(BlobId.from("result-id"), "result", blobFileWriter))
    );

    // When - Then
    assertThatThrownBy(() -> taskContext.getOutput("address")).isInstanceOf(IllegalArgumentException.class);
  }
}
