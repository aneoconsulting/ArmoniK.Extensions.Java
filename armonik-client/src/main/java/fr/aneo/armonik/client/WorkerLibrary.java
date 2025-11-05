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

import fr.aneo.armonik.client.definition.TaskDefinition;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Describes a dynamically loaded worker library to be executed by ArmoniK workers using SDK conventions.
 * <p>
 * {@code WorkerLibrary} enables the ArmoniK SDK's dynamic library loading mechanism, allowing tasks
 * to reference worker implementations packaged as libraries without requiring custom worker container
 * images for each application. This mechanism is language-agnostic: Java clients can submit tasks
 * that execute on workers implemented in different languages (C#, Python, C++, etc.).
 * <p>
 * When tasks are submitted with a worker library configuration, the ArmoniK worker runtime:
 * <ol>
 *   <li>Downloads the library blob referenced by {@link #libraryHandle()}</li>
 *   <li>Extracts the zip file</li>
 *   <li>Locates the artifact at {@link #path()} within the extracted package</li>
 *   <li>Loads the entry point identified by {@link #symbol()} using the worker's runtime loader</li>
 *   <li>Delegates task processing to the loaded implementation</li>
 * </ol>
 * <p>
 * This dynamic loading approach provides several benefits:
 * <ul>
 *   <li><strong>Container reusability:</strong> A single generic worker container image can execute multiple applications</li>
 *   <li><strong>Deployment flexibility:</strong> Application code updates require only new library uploads, not container redeployment</li>
 *   <li><strong>Version isolation:</strong> Different tasks can use different library versions within the same session</li>
 *   <li><strong>Language interoperability:</strong> Java clients can orchestrate tasks executed by workers in any supported language</li>
 * </ul>
 * <p>
 * <strong>SDK Convention Mapping:</strong>
 * <p>
 * According to ArmoniK SDK conventions, worker library information is encoded as task options
 * in the task metadata. This class automatically translates its fields into the required
 * task option keys when {@link #asDeferredTaskOptions()} is called:
 * <ul>
 *   <li>{@code LibraryPath} → {@link #path()}: Path to the library artifact within the package</li>
 *   <li>{@code Symbol} → {@link #symbol()}: Fully qualified name of the entry point symbol</li>
 *   <li>{@code LibraryBlobId} → {@link #libraryHandle()}: Result ID containing the library package</li>
 *   <li>{@code ConventionVersion}: Always set to {@code "v1"} for compatibility tracking</li>
 * </ul>
 * <p>
 * <strong>No-Library Singleton:</strong>
 * <p>
 * {@link #NO_WORKER_LIBRARY} represents the absence of dynamic library loading. When used,
 * tasks will be executed by the default worker implementation built into the worker container,
 * without attempting to load external libraries.
 * <p>
 * <strong>Validation:</strong>
 * <p>
 * The compact constructor enforces all-or-nothing semantics: either all three components
 * ({@code path}, {@code symbol}, {@code libraryHandle}) must be provided with non-blank values,
 * or all must be absent (null). Partial configurations are rejected to prevent ambiguous
 * worker loading behavior.
 *
 * @param path          the path to the library artifact within the zip file.
 *                      This is the relative path from the archive root to the artifact containing the worker
 *                      implementation.
 * @param symbol        the fully qualified name of the entry point symbol to load and instantiate.
 *                      The interpretation is language-specific: a class name in Java or C#,
 *                      a module and class in Python, etc. Examples: {@code "com.example.MyWorker"} (Java),
 *                      {@code "MyNamespace.MyWorker"} (C#)
 * @param libraryHandle a blob handle referencing the uploaded library package
 *                      containing the worker implementation
 *
 * @see TaskDefinition#withWorkerLibrary(WorkerLibrary)
 * @see BlobHandle
 * @see TaskConfiguration#options()
 */
public record WorkerLibrary(
  String path,
  String symbol,
  BlobHandle libraryHandle
) {

  /**
   * Task option key for the library blob identifier in ArmoniK SDK conventions.
   * <p>
   * Maps to the {@link #libraryHandle()}'s blob ID when serialized as task options.
   *
   * @see #asDeferredTaskOptions()
   */
  public static final String LIBRARY_BLOB_ID = "LibraryBlobId";

  /**
   * Task option key for the worker entry point symbol in ArmoniK SDK conventions.
   * <p>
   * Maps to {@link #symbol()} when serialized as task options. The symbol represents
   * the fully qualified name of the entry point to be loaded by the worker runtime,
   * with interpretation depending on the worker's language.
   *
   * @see #asDeferredTaskOptions()
   */
  public static final String SYMBOL = "Symbol";

  /**
   * Task option key for the library artifact path in ArmoniK SDK conventions.
   * <p>
   * Maps to {@link #path()} when serialized as task options. The path specifies
   * the location of the worker artifact within the library package.
   *
   * @see #asDeferredTaskOptions()
   */
  public static final String LIBRARY_PATH = "LibraryPath";

  /**
   * Task option key for the SDK convention version identifier.
   * <p>
   * Always set to {@code "v1"} to indicate the current convention schema version.
   *
   * @see #asDeferredTaskOptions()
   */
  public static final String CONVENTION_VERSION = "ConventionVersion";

  /**
   * Singleton representing the absence of dynamic worker library loading.
   * <p>
   * When a task definition uses this instance, the task will be executed by the default
   * worker implementation compiled into the worker container, without attempting to load
   * any external libraries.
   *
   * <p>
   * Calling {@link #asDeferredTaskOptions()} on this instance returns an empty map,
   * resulting in no library-related task options being set.
   */
  public static final WorkerLibrary NO_WORKER_LIBRARY = new WorkerLibrary(null, null, null);

  /**
   * Validates the worker library configuration for consistency.
   * <p>
   * Enforces all-or-nothing semantics: either all three components must be provided
   * with valid non-blank values, or all must be absent (null). This prevents ambiguous
   * configurations that could lead to worker loading failures.
   * <p>
   * Valid configurations:
   * <ul>
   *   <li>All null: equivalent to {@link #NO_WORKER_LIBRARY}</li>
   *   <li>All non-null with non-blank strings: complete library specification</li>
   * </ul>
   *
   * @throws IllegalArgumentException if partial configuration is detected (some but not all parameters provided),
   *                                  or if any provided string parameter is blank
   */
  public WorkerLibrary {
    long defined = Stream.of(path, symbol, libraryHandle)
                         .filter(Objects::nonNull)
                         .count();

    if (defined > 0 && defined < 3) {
      throw new IllegalArgumentException(
        "Partial WorkerLibrary definition is not allowed: path, symbol and libraryHandle must all be provided together, or all absent.");
    }

    if (defined == 3 && (path.isBlank() || symbol.isBlank())) {
      throw new IllegalArgumentException("Path or Symbol cannot be blank");
    }
  }

  /**
   * Converts this worker library reference into ArmoniK SDK task options asynchronously.
   * <p>
   * This method produces a completion stage that resolves to a map of task option keys and values
   * conforming to ArmoniK SDK conventions. The returned options are merged into the task's
   * configuration during submission to instruct the worker runtime on library loading.
   * <p>
   * The conversion is asynchronous because it requires resolving the library blob's identifier
   * from {@link #libraryHandle()}, which may involve gRPC communication with the ArmoniK cluster.
   * <p>
   * <strong>Produced Task Options (when not {@link #NO_WORKER_LIBRARY}):</strong>
   * <table border="1">
   *   <caption>SDK Convention Task Options Mapping</caption>
   *   <tr>
   *     <th>Task Option Key</th>
   *     <th>Source</th>
   *     <th>Description</th>
   *   </tr>
   *   <tr>
   *     <td>{@code LibraryBlobId}</td>
   *     <td>{@link #libraryHandle()}'s blob ID</td>
   *     <td>Identifier of the result containing the library archive</td>
   *   </tr>
   *   <tr>
   *     <td>{@code Symbol}</td>
   *     <td>{@link #symbol()}</td>
   *     <td>Fully qualified entry point symbol name (language-specific)</td>
   *   </tr>
   *   <tr>
   *     <td>{@code LibraryPath}</td>
   *     <td>{@link #path()}</td>
   *     <td>Path to worker artifact within package</td>
   *   </tr>
   *   <tr>
   *     <td>{@code ConventionVersion}</td>
   *     <td>{@code "v1"}</td>
   *     <td>SDK convention schema version</td>
   *   </tr>
   * </table>
   * <p>
   * <strong>Empty Map Behavior:</strong>
   * <p>
   * If this instance is {@link #NO_WORKER_LIBRARY}, the returned completion stage resolves
   * immediately to an empty map, resulting in no library-related task options being added
   * to the task configuration.
   *
   * @return a completion stage that resolves to a map of task option keys and values
   *         encoding this worker library reference, or an empty map if no library is configured
   * @see TaskConfiguration#options()
   * @see TaskSubmitter#submit(fr.aneo.armonik.client.definition.TaskDefinition)
   */
  CompletionStage<Map<String, String>> asDeferredTaskOptions() {
    if (this.equals(NO_WORKER_LIBRARY)) return completedFuture(Collections.emptyMap());

    return libraryHandle.deferredBlobInfo()
                        .thenApply(blobInfo -> Map.of(
                          LIBRARY_BLOB_ID, blobInfo.id().asString(),
                          SYMBOL, symbol,
                          LIBRARY_PATH, path,
                          CONVENTION_VERSION, "v1"
                        ));
  }
}
