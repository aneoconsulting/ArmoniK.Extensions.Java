package fr.aneo.armonik.worker.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerLibraryTest {

  @Test
  @DisplayName("should create WorkerLibrary when properties contain all the required properties")
  void should_create_WorkerLibrary_from_Map_containing_all_required_properties() {
    // Given
    var properties = Map.of(
      "LibraryBlobId", "1234567890",
      "LibraryPath", "/path/to/library",
      "Symbol", "mySymbol",
      "ConventionVersion", "v1"
    );

    // When
    var workerLibrary = WorkerLibrary.from(properties);

    // Then
    assertThat(workerLibrary.blobId()).isEqualTo(BlobId.from("1234567890"));
    assertThat(workerLibrary.path()).isEqualTo("/path/to/library");
    assertThat(workerLibrary.symbol()).isEqualTo("mySymbol");
    assertThat(workerLibrary.version()).isEqualTo("v1");
    assertThat(workerLibrary.isEmpty()).isFalse();
  }

  @Test
  @DisplayName("should not create WorkerLibrary when 'LibraryPath' is missing")
  void should_not_create_WorkerLibrary_when_LibraryPath_is_missing() {
    // Given
    var properties = Map.of(
      "LibraryBlobId", "1234567890",
      "Symbol", "mySymbol",
      "ConventionVersion", "v1"
    );

    // When / Then
    assertThatThrownBy(() -> WorkerLibrary.from(properties)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should not create WorkerLibrary when 'Symbol' is missing")
  void should_not_create_WorkerLibrary_when_Symbol_is_missing() {
    // Given
    var properties = Map.of(
      "LibraryBlobId", "1234567890",
      "LibraryPath", "/path/to/library",
      "ConventionVersion", "v1"
    );

    // When / Then
    assertThatThrownBy(() -> WorkerLibrary.from(properties)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should not create WorkerLibrary when 'ConventionVersion' is missing")
  void should_not_create_WorkerLibrary_when_ConventionVersion_is_missing() {
    // Given
    var properties = Map.of(
      "LibraryBlobId", "1234567890",
      "LibraryPath", "/path/to/library",
      "Symbol", "mySymbol"
    );

    // When / Then
    assertThatThrownBy(() -> WorkerLibrary.from(properties)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should not create WorkerLibrary when 'LibraryBlobId' is missing")
  void should_not_create_WorkerLibrary_when_LibraryBlobId_is_missing() {
    // Given
    var properties = Map.of(
      "LibraryPath", "/path/to/library",
      "Symbol", "mySymbol",
      "ConventionVersion", "v1"
    );

    // When / Then
    assertThatThrownBy(() -> WorkerLibrary.from(properties)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should not create WorkerLibrary when 'ConventionVersion' is not 'v1'")
  void should_not_create_WorkerLibrary_when_ConventionVersion_is_not_v1() {
    // Given
    var properties = Map.of(
      "LibraryBlobId", "1234567890",
      "LibraryPath", "/path/to/library",
      "Symbol", "mySymbol",
      "ConventionVersion", "adsdff"
    );

    // When / Then
    assertThatThrownBy(() -> WorkerLibrary.from(properties)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("should null when properties does not contains any required properties")
  void should_null_when_properties_contains_any_required_properties() {
    // Given
    var properties = Map.<String, String>of();

    // When
    var workerLibrary = WorkerLibrary.from(properties);

    // Then
    assertThat(workerLibrary.isEmpty()).isTrue();
  }

  @Test
  @DisplayName("should convert WorkerLibrary back to task options")
  void should_convert_WorkerLibrary_to_task_options() {
    // Given
    var properties = Map.of(
      "LibraryBlobId", "1234567890",
      "LibraryPath", "/path/to/library",
      "Symbol", "mySymbol",
      "ConventionVersion", "v1"
    );

    // When - Then
    assertThat(WorkerLibrary.from(properties).asTaskOptions()).isEqualTo(properties);
    assertThat(WorkerLibrary.NO_WORKER_LIBRARY.asTaskOptions()).isEmpty();
  }

  @Test
  @DisplayName("should change only the symbol with withSymbol")
  void should_change_only_the_symbol() {
    // Given
    var workerLibrary = new WorkerLibrary(BlobId.from("1234567890"), "/path/to/library", "mySymbol", "v1");

    // When
    var other = workerLibrary.withSymbol("otherSymbol");

    // Then
    assertThat(other).isEqualTo(new WorkerLibrary(BlobId.from("1234567890"), "/path/to/library", "otherSymbol", "v1"));
    assertThatThrownBy(() -> workerLibrary.withSymbol(" ")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> WorkerLibrary.NO_WORKER_LIBRARY.withSymbol("mySymbol")).isInstanceOf(IllegalStateException.class);
  }
}
