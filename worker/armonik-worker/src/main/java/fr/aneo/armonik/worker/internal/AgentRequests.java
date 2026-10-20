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
package fr.aneo.armonik.worker.internal;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

/**
 * Size limits of the requests sent to the Agent, and grouping of repeated entries under them.
 * <p>
 * The Agent's gRPC server keeps the default limit on received messages, {@value #MAX_REQUEST_SIZE}
 * bytes, and rejects larger requests. The limit applies to the serialized request, so the
 * communication token, the session id and the protobuf framing of each entry count as well as the data.
 * </p>
 */
final class AgentRequests {

  /**
   * Largest serialized request the Agent accepts: {@value} bytes.
   */
  static final int MAX_REQUEST_SIZE = 4 * 1024 * 1024;

  private AgentRequests() {
  }

  /**
   * Returns the room left for repeated entries in a request, given the request without its entries.
   *
   * @param envelope the request with every field set except the repeated entries
   * @return the number of bytes the entries may take
   */
  static int entryBudget(MessageLite envelope) {
    return MAX_REQUEST_SIZE - envelope.getSerializedSize();
  }

  /**
   * Returns the number of bytes an entry takes in a repeated message field, framing included.
   *
   * @param fieldNumber the number of the repeated field
   * @param entry       the entry
   * @return the serialized size of the entry in the request
   */
  static int entrySize(int fieldNumber, MessageLite entry) {
    return CodedOutputStream.computeMessageSize(fieldNumber, entry);
  }

  /**
   * Splits entries, in order, into groups of at most {@code maxEntries} entries whose sizes add up
   * to at most {@code budget} bytes. An entry larger than the budget forms a group of its own, so
   * that the Agent reports the error for this entry only.
   *
   * @param entries    the entries to group
   * @param size       the size of an entry in the request
   * @param budget     the room for entries in one request
   * @param maxEntries the maximum number of entries in one request
   * @param <T>        the entry type
   * @return the groups, in the order of {@code entries}
   */
  static <T> List<List<T>> groupInOrder(List<T> entries, ToIntFunction<T> size, int budget, int maxEntries) {
    var groups = new ArrayList<List<T>>();
    var group = new ArrayList<T>();
    long groupSize = 0;
    for (var entry : entries) {
      var entrySize = size.applyAsInt(entry);
      if (!group.isEmpty() && (group.size() == maxEntries || groupSize + entrySize > budget)) {
        groups.add(group);
        group = new ArrayList<>();
        groupSize = 0;
      }
      group.add(entry);
      groupSize += entrySize;
    }
    if (!group.isEmpty()) groups.add(group);
    return groups;
  }
}
