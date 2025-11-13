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

import fr.aneo.armonik.worker.domain.ArmoniKException;
import fr.aneo.armonik.worker.domain.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads TaskProcessor implementations dynamically from ZIP files.
 * <p>
 * This loader orchestrates the complete dynamic loading process:
 * </p>
 * <ol>
 *   <li>Locates the library ZIP file in the shared data folder</li>
 *   <li>Extracts the ZIP to a temporary directory</li>
 *   <li>Locates the JAR file within the extracted files</li>
 *   <li>Creates a URLClassLoader for the JAR</li>
 *   <li>Loads the TaskProcessor class</li>
 *   <li>Instantiates the TaskProcessor</li>
 *   <li>Returns a LoadedTaskProcessor for cleanup management</li>
 * </ol>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is stateless and thread-safe.
 * </p>
 */
public final class DynamicTaskProcessorLoader {
  private static final Logger logger = LoggerFactory.getLogger(DynamicTaskProcessorLoader.class);

  /**
   * Loads a TaskProcessor from a library ZIP file.
   *
   * @param library    the library metadata containing blob ID, JAR path, and class name
   * @param dataFolder the shared data folder containing the library ZIP file
   * @return a LoadedTaskProcessor wrapping the taskProcessor and its resources
   * @throws ArmoniKException if loading fails at any step
   */
  LoadedTaskProcessor load(WorkerLibrary library, Path dataFolder) {
    logger.info("Loading TaskProcessor: blobId={}, symbol={}", library.blobId(), library.symbol());

    try {
      var zipFile = locateLibraryBlob(library, dataFolder);
      var extractionDir = createTempDirectory(library);

      ZipExtractor.extract(zipFile, extractionDir);

      var jarFile = locateJar(extractionDir, library);
      var classLoader = createClassLoader(jarFile);
      var taskProcessor = loadTaskProcessor(classLoader, library.symbol());

      logger.info("TaskProcessor loaded: {}", library.symbol());
      return new LoadedTaskProcessor(taskProcessor, classLoader, extractionDir);

    } catch (ArmoniKException exception) {
      throw exception;
    } catch (Exception e) {
      throw new ArmoniKException("Failed to load TaskProcessor: " + e.getMessage(), e);
    }
  }

  private Path locateLibraryBlob(WorkerLibrary library, Path dataFolder) {
    var zipFile = PathValidator.resolveWithin(dataFolder, library.blobId().asString());
    PathValidator.validateFile(zipFile);

    return zipFile;
  }

  private Path createTempDirectory(WorkerLibrary library) {
    try {
      return Files.createTempDirectory("armonik-lib-" + library.blobId().asString() + "-");
    } catch (IOException e) {
      throw new ArmoniKException("Failed to create temp directory", e);
    }
  }

  private Path locateJar(Path extractionDir, WorkerLibrary library) {
    var jarFile = PathValidator.resolveDescendant(extractionDir, library.path());
    PathValidator.validateFile(jarFile);

    return jarFile;
  }

  private URLClassLoader createClassLoader(Path jarFile) {
    try {
      var jarUrl = jarFile.toUri().toURL();
      return new URLClassLoader(new URL[]{jarUrl}, TaskProcessor.class.getClassLoader());
    } catch (Exception e) {
      throw new ArmoniKException("Failed to create ClassLoader", e);
    }
  }

  @SuppressWarnings("unchecked")
  private TaskProcessor loadTaskProcessor(URLClassLoader classLoader, String symbol) {
    try {
      Class<?> clazz = classLoader.loadClass(symbol);
      if (!TaskProcessor.class.isAssignableFrom(clazz)) {
        throw new ArmoniKException("Class does not implement TaskProcessor: " + symbol);
      }
      var taskProcessorClass = (Class<? extends TaskProcessor>) clazz;

      return taskProcessorClass.getDeclaredConstructor().newInstance();

    } catch (ClassNotFoundException e) {
      throw new ArmoniKException("TaskProcessor class not found: " + symbol, e);
    } catch (NoSuchMethodException e) {
      throw new ArmoniKException("TaskProcessor must have no-arg constructor: " + symbol, e);
    } catch (Exception e) {
      throw new ArmoniKException("Failed to instantiate TaskProcessor: " + e.getMessage(), e);
    }
  }
}
