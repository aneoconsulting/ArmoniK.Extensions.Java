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
package fr.aneo.armonik.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

public final class BlobHandle {
  private static final Logger logger = LoggerFactory.getLogger(BlobHandle.class);

  private final SessionId sessionId;
  private final String name;
  private final CompletionStage<BlobInfo> deferredBlobInfo;

  BlobHandle(SessionId sessionId, String name, CompletionStage<BlobInfo> deferredBlobInfo) {
    this.sessionId = requireNonNull(sessionId, "sessionId must not be null");
    this.deferredBlobInfo = requireNonNull(deferredBlobInfo, "blobInfo");
    this.name = name;
  }

  public SessionId sessionId() {
    return sessionId;
  }

  public String name() {
    return name;
  }

  /**
   * Returns a completion stage that provides immutable metadata about this blob.
   * <p>
   * The blob information becomes available after the ArmoniK cluster acknowledges
   * the blob creation and assigns identifiers. The returned completion stage may
   * complete exceptionally if blob creation or metadata retrieval fails.
   * <p>
   * This method provides non-blocking access to blob metadata, including the
   * blob identifier and other cluster-assigned properties.
   *
   * @return a completion stage that completes with blob metadata
   * @see BlobInfo
   */
  public CompletionStage<BlobInfo> deferredBlobInfo() {
    return deferredBlobInfo;
  }
}
