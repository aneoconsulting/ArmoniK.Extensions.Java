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
package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.worker.domain.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Wrapper for a dynamically loaded TaskProcessor with associated resources.
 * <p>
 * This record holds the taskProcessor, its ClassLoader, and the temporary extraction directory.
 * When closed, it releases file handles and deletes temporary files.
 * </p>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * try (LoadedProcessor loaded = loader.load(library, zipFile)) {
 *   TaskProcessor taskProcessor = loaded.taskProcessor();
 *   TaskOutcome outcome = taskProcessor.processTask(context);
 *   // ...
 * } // Automatic cleanup: ClassLoader closed, temp files deleted
 * }</pre>
 */
record LoadedTaskProcessor(
  TaskProcessor taskProcessor,
  URLClassLoader classLoader,
  Path extractionDir
) implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(LoadedTaskProcessor.class);

  @Override
  public void close() {
    closeClassLoader();
    deleteExtractionDirectory();
  }

  private void deleteExtractionDirectory() {
    try {
      deleteRecursively(extractionDir);
      logger.debug("Deleted temporary directory: {}", extractionDir);
    } catch (IOException e) {
      logger.warn("Failed to delete temporary directory: {}", extractionDir, e);
    }
  }

  private void closeClassLoader() {
    try {
      classLoader.close();
      logger.debug("ClassLoader closed for Task processor: {}", taskProcessor.getClass().getName());
    } catch (IOException e) {
      logger.warn("Failed to close ClassLoader", e);
    }
  }

  private static void deleteRecursively(Path directory) throws IOException {
    if (!Files.exists(directory)) return;

    try (var walk = Files.walk(directory)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(path -> {
            try {
              Files.delete(path);
            } catch (IOException e) {
              logger.debug("Failed to delete: {}", path, e);
            }
          });
    }
  }
}
