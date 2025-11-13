package fr.aneo.armonik.worker.internal;

import fr.aneo.armonik.worker.domain.ArmoniKException;
import fr.aneo.armonik.worker.domain.TaskOutcome;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
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
    var loader = new DynamicTaskProcessorLoader();
    try (LoadedTaskProcessor loaded = loader.load(library, tempDir)) {

      // then
      var taskProcessor = loaded.taskProcessor();
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
