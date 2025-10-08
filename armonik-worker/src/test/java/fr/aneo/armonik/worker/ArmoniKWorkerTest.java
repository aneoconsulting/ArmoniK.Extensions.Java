package fr.aneo.armonik.worker;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static uk.org.webcompere.systemstubs.SystemStubs.withEnvironmentVariable;
import static uk.org.webcompere.systemstubs.SystemStubs.withEnvironmentVariables;

class ArmoniKWorkerTest {

  private ArmoniKWorker armoniKWorker;

  @BeforeEach
  void setUp() {
    armoniKWorker = new ArmoniKWorker(mock(TaskProcessor.class));
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    armoniKWorker.shutdown();
  }

  @Test
  void should_fallback_to_default_when_env_missing() throws Exception {
    // Given
    withEnvironmentVariables()
               .execute(() -> {
                 // When
                 armoniKWorker.start();
                 // Then
                 assertThat(armoniKWorker.address())
                   .extracting(InetSocketAddress::getHostString, InetSocketAddress::getPort)
                   .containsExactly("0.0.0.0", 8080);
               });
  }

  @Test
  void should_parse_valid_ip_port() throws Exception {
    // Given
    withEnvironmentVariable("ComputePlane__WorkerChannel__Address", "127.0.0.1:50051")
               .execute(() -> {
                 // When
                 armoniKWorker.start();

                 // Then
                 assertThat(armoniKWorker.address())
                   .extracting(InetSocketAddress::getHostString, InetSocketAddress::getPort)
                   .containsExactly("127.0.0.1", 50051);
               });
  }

  @Test
  void should_throw_exception_when_address_is_unparseable() throws Exception {
    // Given
    withEnvironmentVariable("ComputePlane__WorkerChannel__Address", "unparseable")
      .execute(() -> {
        // When / Then
        assertThatThrownBy(armoniKWorker::start)
          .isInstanceOf(IllegalArgumentException.class);
      });
  }


  @Test
  void should_throw_exception_when_port_is_not_numeric() throws Exception {
    // Given
    withEnvironmentVariable("ComputePlane__WorkerChannel__Address", "127.0.0.1:http")
      .execute(() -> {
        // When / Then
        assertThatThrownBy(armoniKWorker::start)
          .isInstanceOf(IllegalArgumentException.class);
      });
  }

  @Test
  void should_throw_exception_when_ip_is_invalid() throws Exception {
    // Given
    withEnvironmentVariable("ComputePlane__WorkerChannel__Address", "999.999.1.1:8080")
      .execute(() -> {
        // When / Then
        assertThatThrownBy(armoniKWorker::start)
          .isInstanceOf(IllegalArgumentException.class);
      });
  }
}
