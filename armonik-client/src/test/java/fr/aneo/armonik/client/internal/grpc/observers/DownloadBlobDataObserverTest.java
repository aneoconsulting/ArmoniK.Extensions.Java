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
package fr.aneo.armonik.client.internal.grpc.observers;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.DownloadResultDataResponse;
import static fr.aneo.armonik.client.TestDataFactory.blobId;
import static fr.aneo.armonik.client.TestDataFactory.sessionId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DownloadBlobDataObserverTest {

  @Test
  @DisplayName("should assemble the chunks in order")
  void should_assemble_the_chunks_in_order() throws Exception {
    // Given
    var observer = new DownloadBlobDataObserver(sessionId(), blobId("blob"));

    // When
    observer.onNext(chunk("Hello"));
    observer.onNext(chunk(", "));
    observer.onNext(chunk("World"));
    observer.onCompleted();

    // Then
    var data = observer.content().toCompletableFuture().get(1, SECONDS);
    assertThat(new String(data, UTF_8)).isEqualTo("Hello, World");
  }

  @Test
  @DisplayName("should return an empty array when no chunk is received")
  void should_return_an_empty_array_without_chunks() throws Exception {
    // Given
    var observer = new DownloadBlobDataObserver(sessionId(), blobId("blob"));

    // When
    observer.onCompleted();

    // Then
    assertThat(observer.content().toCompletableFuture().get(1, SECONDS)).isEmpty();
  }

  @Test
  @DisplayName("should fail when the stream fails")
  void should_fail_when_the_stream_fails() {
    // Given
    var observer = new DownloadBlobDataObserver(sessionId(), blobId("blob"));
    observer.onNext(chunk("partial"));

    // When
    observer.onError(new IllegalStateException("stream broken"));

    // Then
    assertThatThrownBy(() -> observer.content().toCompletableFuture().get(1, SECONDS))
      .hasRootCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("should fill an array of the expected size")
  void should_fill_array_of_expected_size() throws Exception {
    var observer = new DownloadBlobDataObserver(sessionId(), blobId("blob"), 12);
    observer.onNext(chunk("Hello"));
    observer.onNext(chunk(", World"));
    observer.onCompleted();
    assertThat(new String(observer.content().toCompletableFuture().get(1, SECONDS), UTF_8)).isEqualTo("Hello, World");
  }

  @Test
  @DisplayName("should return only the received data when less than expected arrives")
  void should_trim_when_less_data_than_expected() throws Exception {
    var observer = new DownloadBlobDataObserver(sessionId(), blobId("blob"), 100);
    observer.onNext(chunk("short"));
    observer.onCompleted();
    assertThat(new String(observer.content().toCompletableFuture().get(1, SECONDS), UTF_8)).isEqualTo("short");
  }

  @Test
  @DisplayName("should keep every chunk when more data than expected arrives")
  void should_fall_back_when_more_data_than_expected() throws Exception {
    var observer = new DownloadBlobDataObserver(sessionId(), blobId("blob"), 4);
    observer.onNext(chunk("Hel"));
    observer.onNext(chunk("lo, "));
    observer.onNext(chunk("World"));
    observer.onCompleted();
    assertThat(new String(observer.content().toCompletableFuture().get(1, SECONDS), UTF_8)).isEqualTo("Hello, World");
  }

  private static DownloadResultDataResponse chunk(String data) {
    return DownloadResultDataResponse.newBuilder().setDataChunk(ByteString.copyFrom(data, UTF_8)).build();
  }
}
