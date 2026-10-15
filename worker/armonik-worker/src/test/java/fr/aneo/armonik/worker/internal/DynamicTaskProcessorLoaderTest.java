package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.worker.domain.WorkerLibrary;
import fr.aneo.armonik.worker.domain.ArmoniKException;
import fr.aneo.armonik.worker.domain.TaskOutcome;
import fr.aneo.armonik.worker.domain.TaskProcessor;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DynamicTaskProcessorLoaderTest {

  @TempDir
  Path tempDir;

  @Test
  @DisplayName("should load valid TaskProcessor from ZIP library")
  void should_load_valid_task_processor() throws Exception {
    // Given
    var zipFile = createValidLibraryZip();
    var library = WorkerLibrary.from(Map.of(
      WorkerLibrary.LIBRARY_BLOB_ID, zipFile.getFileName().toString(),
      WorkerLibrary.LIBRARY_PATH, "lib/processor.jar",
      WorkerLibrary.SYMBOL, "TestTaskProcessor",
      WorkerLibrary.CONVENTION_VERSION, "v1"
    ));

    // When
    try (var loader = new DynamicTaskProcessorLoader()) {
      var taskProcessor = loader.load(library, tempDir);

      // then
      assertThat(taskProcessor).isNotNull();
      assertThat(taskProcessor.getClass().getName()).isEqualTo("TestTaskProcessor");

      var outcome = taskProcessor.processTask(null);
      assertThat(outcome).isEqualTo(TaskOutcome.SUCCESS);
    }
  }

  @Test
  @DisplayName("should reject class that does not implement TaskProcessor")
  void should_reject_class_not_implementing_task_processor() throws Exception {
    // Given
    var zipFile = createLibraryWithWrongInterface();
    var library = WorkerLibrary.from(Map.of(
      WorkerLibrary.LIBRARY_BLOB_ID, zipFile.getFileName().toString(),
      WorkerLibrary.LIBRARY_PATH, "lib/processor.jar",
      WorkerLibrary.SYMBOL, "InvalidProcessor",
      WorkerLibrary.CONVENTION_VERSION, "v1"
    ));

    // When/Then: Should reject
    var loader = new DynamicTaskProcessorLoader();
    assertThatThrownBy(() -> loader.load(library, tempDir))
      .isInstanceOf(ArmoniKException.class)
      .hasMessageContaining("does not implement TaskProcessor");
  }

  @Test
  @DisplayName("should reject class without no-arg constructor")
  void should_reject_class_without_no_arg_constructor() throws Exception {
    // Given
    Path zipFile = createLibraryWithoutNoArgConstructor();

    var library = WorkerLibrary.from(Map.of(
      WorkerLibrary.LIBRARY_BLOB_ID, zipFile.getFileName().toString(),
      WorkerLibrary.LIBRARY_PATH, "lib/processor.jar",
      WorkerLibrary.SYMBOL, "NoDefaultConstructorProcessor",
      WorkerLibrary.CONVENTION_VERSION, "v1"
    ));

    // When/Then
    var loader = new DynamicTaskProcessorLoader();
    assertThatThrownBy(() -> loader.load(library, tempDir))
      .isInstanceOf(ArmoniKException.class)
      .hasMessageContaining("no-arg constructor")
      .hasMessageContaining("NoDefaultConstructorProcessor");
  }

  @Test
  @DisplayName("should reuse cached library and create a new TaskProcessor instance per load")
  void should_reuse_cached_library() throws Exception {
    // Given
    var zipFile = createValidLibraryZip();
    var library = libraryFor(zipFile, "TestTaskProcessor");

    try (var loader = new DynamicTaskProcessorLoader()) {
      var first = loader.load(library, tempDir);
      Files.delete(zipFile);

      // When
      var second = loader.load(library, tempDir);

      // Then
      assertThat(second).isNotSameAs(first);
      assertThat(second.getClass()).isSameAs(first.getClass());
      assertThat(loader.cachedLibraryCount()).isEqualTo(1);
    }
  }

  @Test
  @DisplayName("should evict and close least recently used library when capacity is exceeded")
  void should_evict_least_recently_used_library() throws Exception {
    // Given
    var zipA = createValidLibraryZip();
    var zipB = Files.copy(zipA, tempDir.resolve("other-library.zip"));
    var zipC = Files.copy(zipA, tempDir.resolve("third-library.zip"));

    try (var loader = new DynamicTaskProcessorLoader(2)) {
      var processorA = loader.load(libraryFor(zipA, "TestTaskProcessor"), tempDir);
      var processorB = loader.load(libraryFor(zipB, "TestTaskProcessor"), tempDir);
      loader.load(libraryFor(zipA, "TestTaskProcessor"), tempDir);

      // When
      loader.load(libraryFor(zipC, "TestTaskProcessor"), tempDir);

      // Then
      assertThat(loader.cachedLibraryCount()).isEqualTo(2);
      assertThat(jarOf(processorA)).exists();
      assertThat(jarOf(processorB)).doesNotExist();
    }
  }

  @Test
  @DisplayName("should release all cached libraries on close")
  void should_release_libraries_on_close() throws Exception {
    // Given
    var library = libraryFor(createValidLibraryZip(), "TestTaskProcessor");
    var loader = new DynamicTaskProcessorLoader();
    var taskProcessor = loader.load(library, tempDir);

    // When
    loader.close();

    // Then
    assertThat(loader.cachedLibraryCount()).isZero();
    assertThat(jarOf(taskProcessor)).doesNotExist();
  }

  @Test
  @DisplayName("should not cache a library whose ZIP file is missing")
  void should_not_cache_missing_library() {
    // Given
    var library = libraryFor(tempDir.resolve("missing.zip"), "TestTaskProcessor");

    try (var loader = new DynamicTaskProcessorLoader()) {
      // When / Then
      assertThatThrownBy(() -> loader.load(library, tempDir)).isInstanceOf(ArmoniKException.class);
      assertThat(loader.cachedLibraryCount()).isZero();
    }
  }

  @Test
  @DisplayName("should reject capacity lower than 1")
  void should_reject_invalid_capacity() {
    assertThatThrownBy(() -> new DynamicTaskProcessorLoader(0)).isInstanceOf(IllegalArgumentException.class);
  }

  private static WorkerLibrary libraryFor(Path zipFile, String symbol) {
    return WorkerLibrary.from(Map.of(
      WorkerLibrary.LIBRARY_BLOB_ID, zipFile.getFileName().toString(),
      WorkerLibrary.LIBRARY_PATH, "lib/processor.jar",
      WorkerLibrary.SYMBOL, symbol,
      WorkerLibrary.CONVENTION_VERSION, "v1"
    ));
  }

  private static Path jarOf(TaskProcessor taskProcessor) throws Exception {
    var classLoader = (URLClassLoader) taskProcessor.getClass().getClassLoader();
    return Path.of(classLoader.getURLs()[0].toURI());
  }

  /**
   * Creates a ZIP file containing a JAR with a compiled TaskProcessor.
   * Structure:
   *   test-library.zip
   *   └── lib/
   *       └── taskProcessor.jar
   *           └── TestTaskProcessor.class
   */
  private Path createValidLibraryZip() throws Exception {
    String sourceCode = """
      import fr.aneo.armonik.worker.domain.TaskProcessor;
      import fr.aneo.armonik.worker.domain.TaskContext;
      import fr.aneo.armonik.worker.domain.TaskOutcome;
import fr.aneo.armonik.worker.domain.TaskProcessor;

      public class TestTaskProcessor implements TaskProcessor {
        public TestTaskProcessor() {}

        @Override
        public TaskOutcome processTask(TaskContext context) {
          return TaskOutcome.SUCCESS;
        }
      }
      """;

    return compileAndPackage("TestTaskProcessor", sourceCode);
  }

  private Path createLibraryWithWrongInterface() throws Exception {
    // Compile a class that does NOT implement TaskProcessor
    String sourceCode = """
    public class InvalidProcessor {
      public InvalidProcessor() {}

      public void doSomething() {
        System.out.println("Not a TaskProcessor!");
      }
    }
    """;

    return compileAndPackage("InvalidProcessor", sourceCode);
  }

  private Path createLibraryWithoutNoArgConstructor() throws Exception {
    // Compile a TaskProcessor that requires a parameter in constructor
    String sourceCode = """
    import fr.aneo.armonik.worker.domain.TaskProcessor;
    import fr.aneo.armonik.worker.domain.TaskContext;
    import fr.aneo.armonik.worker.domain.TaskOutcome;
import fr.aneo.armonik.worker.domain.TaskProcessor;

    public class NoDefaultConstructorProcessor implements TaskProcessor {
      private final String config;

      // Only constructor requires parameter - no no-arg constructor
      public NoDefaultConstructorProcessor(String config) {
        this.config = config;
      }

      @Override
      public TaskOutcome processTask(TaskContext context) {
        return TaskOutcome.SUCCESS;
      }
    }
    """;

    return compileAndPackage("NoDefaultConstructorProcessor", sourceCode);
  }


  /**
   * Compiles Java source and packages it into a ZIP library.
   *
   * @param className simple class name (e.g., "TestTaskProcessor")
   * @param sourceCode complete Java source code
   * @return Path to created ZIP file
   */
  private Path compileAndPackage(String className, String sourceCode) throws Exception {
    var sourceDir = tempDir.resolve("src-" + className);
    var sourceFile = sourceDir.resolve(className + ".java");
    var classesDir = tempDir.resolve("classes-" + className);

    Files.createDirectories(sourceDir);
    Files.writeString(sourceFile, sourceCode);
    Files.createDirectories(classesDir);

    var compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new AssumptionViolatedException("JDK required for this test");
    }

    int result = compiler.run(null, null, null,
      "-d", classesDir.toString(),
      "-cp", System.getProperty("java.class.path"),
      sourceFile.toString()
    );

    if (result != 0) {
      throw new RuntimeException("Compilation failed for " + className);
    }

    var jarFile = tempDir.resolve(className + ".jar");
    try (var jos = new JarOutputStream(Files.newOutputStream(jarFile))) {
      Path classFile = classesDir.resolve(className + ".class");
      jos.putNextEntry(new JarEntry(className + ".class"));
      Files.copy(classFile, jos);
      jos.closeEntry();
    }

    var zipFile = tempDir.resolve(className + "-library.zip");
    try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
      zos.putNextEntry(new ZipEntry("lib/"));
      zos.closeEntry();

      zos.putNextEntry(new ZipEntry("lib/processor.jar"));
      Files.copy(jarFile, zos);
      zos.closeEntry();
    }

    return zipFile;
  }
}
