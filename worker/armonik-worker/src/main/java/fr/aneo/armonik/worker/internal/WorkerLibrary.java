package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.worker.domain.BlobId;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Configuration for dynamically loading worker libraries at runtime.
 * <p>
 * This record encapsulates the information required to locate, retrieve, and instantiate
 * a {@link fr.aneo.armonik.worker.domain.TaskProcessor} implementation from a ZIP archive
 * stored in ArmoniK's blob storage. It follows the ArmoniK SDK conventions for cross-language
 * compatibility.
 * </p>
 *
 * <h2>ArmoniK SDK Conventions</h2>
 * <p>
 * The ArmoniK SDK defines a standardized set of task options for dynamic library loading that
 * work consistently across different language implementations (Java, C#, Python, C++). These
 * conventions enable clients to submit tasks that can be executed by workers in different languages
 * without knowing the specific runtime details.
 * </p>
 *
 * <h3>Required Task Options</h3>
 * <dl>
 *   <dt>{@value #LIBRARY_BLOB_ID}</dt>
 *   <dd>The identifier of the blob (result) containing the ZIP archive with the worker library
 *       and its dependencies. This blob must be created before task submission using the Results
 *       service and included in the task's data dependencies.</dd>
 *
 *   <dt>{@value #LIBRARY_PATH}</dt>
 *   <dd>The relative path within the ZIP archive to the main library artifact.</dd>
 *
 *   <dt>{@value #SYMBOL}</dt>
 *   <dd>The language-specific symbol identifying the entry point to instantiate.
 *       <ul>
 *         <li><strong>Java</strong>: Fully qualified class name implementing TaskProcessor
 *             (e.g., {@code com.example.MyTaskProcessor})</li>
 *         <li><strong>C#</strong>: Fully qualified class name
 *             (e.g., {@code MyNamespace.MyTaskProcessor})</li>
 *       </ul>
 *   </dd>
 *
 *   <dt>{@value #CONVENTION_VERSION}</dt>
 *   <dd>The version of the SDK conventions being used. Currently only {@code "v1"} is supported.
 *       This allows future evolution of the conventions while maintaining backward compatibility.</dd>
 * </dl>
 *
 * <h2>ZIP Archive Structure</h2>
 * <p>
 * The ZIP archive referenced by {@code LibraryBlobId} should contain the main library artifact
 * and all its dependencies. Example structure for Java:
 * </p>
 *
 * <h2>Validation</h2>
 * <p>
 * The {@link #from(Map)} factory method validates that:
 * </p>
 * <ul>
 *   <li>All required keys are present and non-blank</li>
 *   <li>The convention version is supported (currently only "v1")</li>
 * </ul>
 * <p>
 * Missing or invalid configuration throws {@link IllegalArgumentException}.
 * </p>
 *
 * @param blobId  the identifier of the blob containing the library ZIP archive
 * @param path    the relative path to the library artifact within the ZIP
 * @param symbol  the language-specific symbol identifying the entry point class
 * @param version the SDK convention version (currently "v1")
 * @see fr.aneo.armonik.worker.internal.DynamicTaskProcessorLoader
 * @see <a href="https://armonik.readthedocs.io/en/latest/">ArmoniK SDK Conventions</a>
 */
public record WorkerLibrary(
  BlobId blobId,
  String path,
  String symbol,
  String version
) {

  /**
   * Task option key for the library blob identifier in ArmoniK SDK conventions.
   * <p>
   * Value: {@value}
   * </p>
   * <p>
   * This key identifies the blob (result) containing the ZIP archive with the worker library
   * and all its dependencies. The blob must be created using the Results service before task
   * submission and must be included in the task's data dependencies.
   * </p>
   *
   * @see #LIBRARY_PATH
   * @see #SYMBOL
   */
  public static final String LIBRARY_BLOB_ID = "LibraryBlobId";

  /**
   * Task option key for the worker entry point symbol in ArmoniK SDK conventions.
   * <p>
   * Value: {@value}
   * </p>
   * <p>
   * The symbol represents the fully qualified identifier of the entry point to be loaded
   * by the worker runtime. Interpretation is language-specific:
   * </p>
   * <ul>
   *   <li><strong>Java</strong>: Fully qualified class name
   *       (e.g., {@code com.example.MyTaskProcessor})</li>
   *   <li><strong>C#</strong>: Fully qualified type name
   *       (e.g., {@code MyNamespace.MyProcessor})</li>
   *   <li><strong>C++</strong>: Exported symbol name from shared library</li>
   * </ul>
   *
   * @see #LIBRARY_PATH
   * @see #LIBRARY_BLOB_ID
   */
  public static final String SYMBOL = "Symbol";

  /**
   * Task option key for the library artifact path in ArmoniK SDK conventions.
   * <p>
   * Value: {@value}
   * </p>
   * <p>
   * The path specifies the relative location of the main worker artifact within the
   * ZIP archive identified by {@link #LIBRARY_BLOB_ID}. The path is relative to the
   * root of the ZIP archive.
   * </p>
   * <p>
   * Examples by language:
   * </p>
   * <ul>
   *   <li><strong>Java</strong>: {@code my-processor.jar} or {@code lib/my-processor.jar}</li>
   *   <li><strong>C#</strong>: {@code MyProcessor.dll} or {@code bin/MyProcessor.dll}</li>
   * </ul>
   *
   * @see #LIBRARY_BLOB_ID
   * @see #SYMBOL
   */
  public static final String LIBRARY_PATH = "LibraryPath";

  /**
   * Task option key for the SDK convention version identifier.
   * <p>
   * Value: {@value}
   * </p>
   * <p>
   * Indicates the version of the ArmoniK SDK conventions being used. This allows the
   * system to evolve conventions over time while maintaining backward compatibility.
   * </p>
   * <p>
   * Currently supported versions:
   * </p>
   * <ul>
   *   <li><strong>v1</strong>: Initial convention version with keys:
   *       LibraryBlobId, LibraryPath, Symbol, ConventionVersion</li>
   * </ul>
   * <p>
   * Future versions may add additional keys or change validation rules while remaining
   * compatible with existing workers that check this version.
   * </p>
   */
  public static final String CONVENTION_VERSION = "ConventionVersion";

  /**
   * Checks if this WorkerLibrary instance contains no library information.
   * <p>
   * A library is considered empty if all its components (blobId, path, symbol, version)
   * are null. An empty library indicates that the task should be processed using a
   * statically configured TaskProcessor rather than dynamic loading.
   * </p>
   *
   * @return {@code true} if all components are null, {@code false} otherwise
   */
  public boolean isEmpty() {
    return blobId == null && path == null && symbol == null && version == null;
  }

  /**
   * Creates a WorkerLibrary instance from task options properties.
   * <p>
   * This factory method extracts library configuration from the task options map and
   * validates that all required information is present and valid. If no library-related
   * keys are present in the properties, returns an empty WorkerLibrary.
   * </p>
   *
   * <h4>Validation Rules</h4>
   * <ul>
   *   <li>If any library key is present, all four keys must be present and non-blank</li>
   *   <li>The {@code ConventionVersion} must be exactly {@code "v1"}</li>
   *   <li>Empty or blank values are treated as missing</li>
   * </ul>
   *
   * @param properties the task options map containing library configuration keys
   * @return a new {@link WorkerLibrary} instance, or an empty library if no configuration is present
   * @throws IllegalArgumentException if library configuration is incomplete (some but not all keys present)
   * @throws IllegalArgumentException if the convention version is not supported
   * @see #isEmpty()
   */
  public static WorkerLibrary from(Map<String, String> properties) {
    if (properties == null || properties.isEmpty() || (
      !properties.containsKey(LIBRARY_BLOB_ID) &&
        !properties.containsKey(LIBRARY_PATH) &&
        !properties.containsKey(SYMBOL) &&
        !properties.containsKey(CONVENTION_VERSION))) {
      return new WorkerLibrary(null, null, null, null);
    }
    validateProperties(properties);

    return new WorkerLibrary(
      BlobId.from(properties.get(LIBRARY_BLOB_ID)),
      properties.get(LIBRARY_PATH),
      properties.get(SYMBOL),
      properties.get(CONVENTION_VERSION)
    );
  }

  /**
   * Validates that all required library configuration properties are present and valid.
   * <p>
   * This method is called by {@link #from(Map)} when at least one library key is detected.
   * It ensures that incomplete configurations are rejected early with clear error messages.
   * </p>
   *
   * @param properties the task options map to validate
   * @throws IllegalArgumentException if any required key is missing, blank, or if the
   *                                  convention version is not supported
   */
  private static void validateProperties(Map<String, String> properties) {
    var missing = Stream.of(CONVENTION_VERSION, LIBRARY_PATH, SYMBOL, LIBRARY_BLOB_ID)
                        .filter(key -> !properties.containsKey(key) || isBlank(properties.get(key)))
                        .toList();

    if (!missing.isEmpty())
      throw new IllegalArgumentException("Incomplete Worker Library information. Missing keys: " + String.join(", ", missing));

    if (!properties.get(CONVENTION_VERSION).equals("v1"))
      throw new IllegalArgumentException("Wrong ConventionVersion. Supported version: 'v1', actual: " + properties.get(CONVENTION_VERSION));
  }

  /**
   * Checks if a string is null or contains only whitespace.
   *
   * @param s the string to check
   * @return {@code true} if the string is null or blank, {@code false} otherwise
   */
  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }
}
