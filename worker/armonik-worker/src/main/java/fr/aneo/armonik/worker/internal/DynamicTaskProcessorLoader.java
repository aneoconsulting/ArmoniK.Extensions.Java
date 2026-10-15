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

import fr.aneo.armonik.worker.domain.WorkerLibrary;
import fr.aneo.armonik.worker.domain.ArmoniKException;
import fr.aneo.armonik.worker.domain.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Loads TaskProcessor implementations dynamically from ZIP files, caching the loaded libraries.
 * <p>
 * The first time a library is requested, the loader:
 * </p>
 * <ol>
 *   <li>Locates the library ZIP file in the shared data folder</li>
 *   <li>Extracts the ZIP to a temporary directory</li>
 *   <li>Locates the JAR file within the extracted files</li>
 *   <li>Creates a URLClassLoader for the JAR</li>
 * </ol>
 * <p>
 * The extraction directory and the ClassLoader are then cached, keyed by the library blob ID and
 * JAR path. A blob is immutable once completed, so a cached entry never becomes stale. Later tasks
 * using the same library skip the extraction and reuse the already loaded classes.
 * </p>
 * <p>
 * Each call to {@link #load(WorkerLibrary, Path)} creates a new TaskProcessor instance, so no
 * instance state is shared between tasks. Static state of the library classes is shared, since
 * they are loaded once per cached library.
 * </p>
 *
 * <h2>Eviction</h2>
 * <p>
 * The cache holds at most {@code capacity} libraries. When full, the least recently used library
 * is closed: its ClassLoader is closed and its extraction directory deleted. {@link #close()}
 * releases all cached libraries.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe. Eviction assumes that a library is not in use by an in-flight task
 * when it becomes the least recently used one, which holds as long as the number of concurrent
 * tasks does not exceed the capacity. The Agent sends one task at a time to a worker.
 * </p>
 */
public final class DynamicTaskProcessorLoader implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DynamicTaskProcessorLoader.class);

  /**
   * Default maximum number of cached libraries: {@value}.
   */
  public static final int DEFAULT_CAPACITY = 4;

  private final Map<LibraryKey, LoadedLibrary> libraries;

  /**
   * Creates a loader caching at most {@link #DEFAULT_CAPACITY} libraries.
   */
  public DynamicTaskProcessorLoader() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * Creates a loader caching at most {@code capacity} libraries.
   *
   * @param capacity maximum number of cached libraries; must be at least 1
   * @throws IllegalArgumentException if {@code capacity} is less than 1
   */
  public DynamicTaskProcessorLoader(int capacity) {
    if (capacity < 1) throw new IllegalArgumentException("capacity must be at least 1, actual: " + capacity);
    this.libraries = new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<LibraryKey, LoadedLibrary> eldest) {
        if (size() <= capacity) return false;
        logger.info("Evicting library from cache: blobId={}, path={}", eldest.getKey().blobId(), eldest.getKey().path());
        eldest.getValue().close();
        return true;
      }
    };
  }

  /**
   * Creates a new TaskProcessor instance from a library, loading the library if it is not cached.
   *
   * @param library    the library metadata containing blob ID, JAR path, and class name
   * @param dataFolder the shared data folder containing the library ZIP file; only read when
   *                   the library is not cached
   * @return a new TaskProcessor instance
   * @throws ArmoniKException if loading fails at any step
   */
  synchronized TaskProcessor load(WorkerLibrary library, Path dataFolder) {
    var key = new LibraryKey(library.blobId().asString(), library.path());
    var loadedLibrary = libraries.get(key);
    if (loadedLibrary == null) {
      loadedLibrary = loadLibrary(library, dataFolder);
      libraries.put(key, loadedLibrary);
    } else {
      logger.info("Using cached library: blobId={}, path={}", key.blobId(), key.path());
    }

    var taskProcessor = loadTaskProcessor(loadedLibrary.classLoader(), library.symbol());
    logger.info("TaskProcessor loaded: {}", library.symbol());
    return taskProcessor;
  }

  /**
   * Closes all cached libraries and empties the cache.
   */
  @Override
  public synchronized void close() {
    logger.info("Closing {} cached libraries", libraries.size());
    libraries.values().forEach(LoadedLibrary::close);
    libraries.clear();
  }

  synchronized int cachedLibraryCount() {
    return libraries.size();
  }

  private LoadedLibrary loadLibrary(WorkerLibrary library, Path dataFolder) {
    logger.info("Loading library: blobId={}, path={}", library.blobId(), library.path());

    Path extractionDir = null;
    URLClassLoader classLoader = null;
    try {
      var zipFile = locateLibraryBlob(library, dataFolder);
      extractionDir = createTempDirectory(library);

      ZipExtractor.extract(zipFile, extractionDir);

      var jarFile = locateJar(extractionDir, library);
      classLoader = createClassLoader(jarFile);
      return new LoadedLibrary(classLoader, extractionDir);

    } catch (Exception e) {
      if (extractionDir != null) new LoadedLibrary(classLoader, extractionDir).close();
      if (e instanceof ArmoniKException armoniKException) throw armoniKException;
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
    } catch (ArmoniKException e) {
      throw e;
    } catch (Exception e) {
      throw new ArmoniKException("Failed to instantiate TaskProcessor: " + e.getMessage(), e);
    }
  }

  private record LibraryKey(String blobId, String path) {
  }
}
