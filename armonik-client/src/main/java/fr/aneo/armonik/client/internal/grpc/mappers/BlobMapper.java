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
package fr.aneo.armonik.client.internal.grpc.mappers;

import com.google.protobuf.UnsafeByteOperations;
import fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.CreateResultsMetaDataRequest.ResultCreate;
import fr.aneo.armonik.client.definition.blob.BlobDefinition;
import fr.aneo.armonik.client.BlobId;
import fr.aneo.armonik.client.SessionId;

import java.util.List;

import static fr.aneo.armonik.api.grpc.v1.results.ResultsCommon.*;

public final class BlobMapper {

  private BlobMapper() {
  }

  public static CreateResultsMetaDataRequest toResultMetaDataRequest(SessionId sessionId, List<BlobDefinition> blobDefinitions) {
    var metadata = blobDefinitions.stream()
                              .map(blobDefinition -> ResultCreate.newBuilder()
                                                                 .setName(blobDefinition.name())
                                                                 .setManualDeletion(blobDefinition.manualDeletion())
                                                                 .build())
                              .toList();
    return CreateResultsMetaDataRequest.newBuilder()
                                       .setSessionId(sessionId.asString())
                                       .addAllResults(metadata)
                                       .build();
  }

  public static DownloadResultDataRequest toDownloadResultDataRequest(SessionId sessionId, BlobId blobId) {
    return DownloadResultDataRequest.newBuilder()
                                    .setSessionId(sessionId.asString()).setResultId(blobId.asString())
                                    .build();
  }

  public static UploadResultDataRequest toUploadResultDataRequest(byte[] data, int offset, int size) {
    return UploadResultDataRequest.newBuilder()
                                  .setDataChunk(UnsafeByteOperations.unsafeWrap(data, offset, size))
                                  .build();
  }

  public static UploadResultDataRequest toUploadResultDataIdentifierRequest(SessionId sessionId, BlobId blobId) {
    return UploadResultDataRequest.newBuilder()
                                  .setId(UploadResultDataRequest.ResultIdentifier.newBuilder()
                                                                                 .setResultId(blobId.asString())
                                                                                 .setSessionId(sessionId.asString()))
                                  .build();
  }
}
