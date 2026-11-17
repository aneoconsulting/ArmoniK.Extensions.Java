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
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.DownloadResultDataRequest;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.DownloadResultDataResponse;
import static fr.aneo.armonik.client.TestDataFactory.blobId;
import static fr.aneo.armonik.client.TestDataFactory.sessionId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
  @DisplayName("should fail as soon as the data exceeds the size an array can hold")
  void should_fail_once_the_data_is_too_large() {
    // Given
    var observer = new DownloadBlobDataObserver(sessionId(), blobId("blob"), 8);
    observer.onNext(chunk("12345"));

    // When
    observer.onNext(chunk("6789"));

    // Then: the result fails before the stream ends, and later chunks or completion change nothing
    assertThatThrownBy(() -> observer.content().toCompletableFuture().get(1, SECONDS))
      .hasRootCauseInstanceOf(IllegalStateException.class)
      .hasMessageContaining("blob");
    observer.onNext(chunk("more"));
    observer.onCompleted();
    assertThatThrownBy(() -> observer.content().toCompletableFuture().get(1, SECONDS))
      .hasRootCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("should cancel the call once the data is too large, and ignore the cancellation that follows")
  @SuppressWarnings("unchecked")
  void should_cancel_the_call_once_the_data_is_too_large() {
    // Given
    var call = (ClientCallStreamObserver<DownloadResultDataRequest>) mock(ClientCallStreamObserver.class);
    var observer = new DownloadBlobDataObserver(sessionId(), blobId("blob"), 8);
    observer.beforeStart(call);
    observer.onNext(chunk("12345"));
    verify(call, never()).cancel(any(), any());

    // When
    observer.onNext(chunk("6789"));

    // Then: the call is cancelled, and its CANCELLED error does not replace the size error
    verify(call).cancel(any(), isNull());
    observer.onError(Status.CANCELLED.asRuntimeException());
    assertThatThrownBy(() -> observer.content().toCompletableFuture().get(1, SECONDS))
      .hasRootCauseInstanceOf(IllegalStateException.class);
  }

  private static DownloadResultDataResponse chunk(String data) {
    return DownloadResultDataResponse.newBuilder().setDataChunk(ByteString.copyFrom(data, UTF_8)).build();
  }
}
