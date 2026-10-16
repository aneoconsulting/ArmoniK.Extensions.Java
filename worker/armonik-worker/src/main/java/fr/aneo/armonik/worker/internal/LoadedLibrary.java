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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * An extracted worker library and the ClassLoader reading its JAR.
 * <p>
 * Instances are cached by {@link DynamicTaskProcessorLoader} and shared by all tasks using the
 * same library. Closing releases the JAR file handles and deletes the extraction directory.
 * </p>
 */
record LoadedLibrary(
  URLClassLoader classLoader,
  Path extractionDir
) implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(LoadedLibrary.class);

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
    if (classLoader == null) return;
    try {
      classLoader.close();
      logger.debug("ClassLoader closed for library: {}", extractionDir);
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
