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
package fr.aneo.armonik.worker.domain;

import java.util.Objects;

/**
 * Immutable identifier for a session within the ArmoniK distributed computing platform.
 * <p>
 * Session identifiers are assigned by the ArmoniK cluster during session creation and
 * uniquely identify sessions across the entire cluster.
 */
public final class SessionId {
  private final String id;

  private SessionId(String id) {
    this.id = id;
  }

  /**
   * Returns the string representation of this session identifier.
   * <p>
   * This method provides access to the underlying string identifier as used by
   * the ArmoniK cluster's gRPC API.
   *
   * @return the session identifier as a string
   */
  public String asString() {
    return id;
  }

  public static SessionId from(String sessionId) {
    return new SessionId(sessionId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (SessionId) obj;
    return Objects.equals(this.id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return "SessionId{" +
      "id='" + id + '\'' +
      '}';
  }
}
