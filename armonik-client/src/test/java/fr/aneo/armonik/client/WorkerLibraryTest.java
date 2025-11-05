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
package fr.aneo.armonik.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static fr.aneo.armonik.client.TestDataFactory.blobHandle;
import static fr.aneo.armonik.client.WorkerLibrary.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerLibraryTest {


  @Test
  @DisplayName("should return empty map when no library is defined")
  void should_return_empty_map_when_no_library_is_defined() {
    // Given
    var workerLibrary = new WorkerLibrary(null, null, null);

    // When
    var deferredTaskOptions = workerLibrary.asDeferredTaskOptions();

    // Then
    assertThat(deferredTaskOptions.toCompletableFuture().join()).isEmpty();
  }

  @Test
  @DisplayName("should not accept partial definition of worker library")
  void should_not_accept_partial_definition_of_worker_library() {
    assertThatThrownBy(() -> new WorkerLibrary(null, "Symbol", blobHandle("sessionId", "libraryBlobId")))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new WorkerLibrary("path", null, blobHandle("sessionId", "libraryBlobId")))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new WorkerLibrary("path", "Symbol", null))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should not accept blank path or symbol")
  void should_not_accept_blank_path_or_symbol() {
    assertThatThrownBy(() -> new WorkerLibrary(" ", "Symbol", blobHandle("sessionId", "libraryBlobId")))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new WorkerLibrary("path", " ", blobHandle("sessionId", "libraryBlobId")))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should return well defined task option for Worker Library")
  void should_return_well_defined_task_option_for_worker_library() {
    // Given
    var workerLibrary = new WorkerLibrary("path", "Symbol", blobHandle("sessionId", "libraryBlobId"));

    // When
    var deferredTaskOptions = workerLibrary.asDeferredTaskOptions();
    assertThat(deferredTaskOptions.toCompletableFuture().join()).containsExactlyInAnyOrderEntriesOf(
      Map.of(
        CONVENTION_VERSION, "v1",
        LIBRARY_PATH, "path",
        SYMBOL, "Symbol",
        LIBRARY_BLOB_ID, "libraryBlobId"
      )
    );
  }
}
