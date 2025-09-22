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
package fr.aneo.armonik.client.blob.event;

import fr.aneo.armonik.api.grpc.v1.FiltersCommon.FilterString;
import fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionRequest;
import fr.aneo.armonik.api.grpc.v1.events.EventsGrpc;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFilters.FilterField;
import fr.aneo.armonik.client.blob.BlobHandle;
import fr.aneo.armonik.client.blob.BlobMetadata;
import fr.aneo.armonik.client.blob.BlobService;
import fr.aneo.armonik.client.session.SessionHandle;
import fr.aneo.armonik.client.util.Futures;
import io.grpc.ManagedChannel;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import static fr.aneo.armonik.api.grpc.v1.FiltersCommon.FilterStringOperator.FILTER_STRING_OPERATOR_EQUAL;
import static fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventsEnum.EVENTS_ENUM_NEW_RESULT;
import static fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventsEnum.EVENTS_ENUM_RESULT_STATUS_UPDATE;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawEnumField.RESULT_RAW_ENUM_FIELD_RESULT_ID;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsFilters.Filters;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsFilters.FiltersAnd;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public final class DefaultBlobCompletionEventWatcher implements BlobCompletionEventWatcher {

  private final EventsGrpc.EventsStub eventsStub;
  private final BlobService blobService;

  public DefaultBlobCompletionEventWatcher(ManagedChannel channel, BlobService blobService) {
    this.eventsStub = EventsGrpc.newStub(channel);
    this.blobService = blobService;
  }

  @Override
  public CompletionStage<Void> watch(SessionHandle sessionHandle,
                                     List<BlobHandle> blobHandles,
                                     BlobCompletionListener listener) {
    requireNonNull(sessionHandle, "sessionHandle must not be null");
    requireNonNull(blobHandles, "blobHandles must not be null");
    requireNonNull(listener, "listener must not be null");

    CompletableFuture<Void> completion = new CompletableFuture<>();

    Futures.allOf(blobHandles.stream().map(BlobHandle::metadata).toList())
           .thenAccept(metadataList -> {
             var metadataById = new ConcurrentHashMap<>(metadataList.stream().collect(toMap(BlobMetadata::id, identity())));

             if (metadataById.isEmpty()) {
               completion.complete(null);
             } else {
               var request = createEventSubscriptionRequest(sessionHandle.id(), metadataList);
               eventsStub.getEvents(
                 request,
                 new BlobCompletionEventSubscriber(sessionHandle, metadataById, completion, blobService, listener)
               );
             }
           })
           .exceptionally(ex -> {
             completion.completeExceptionally(ex);
             return null;
           });

    return completion;
  }

  private static EventSubscriptionRequest createEventSubscriptionRequest(UUID sessionId, List<BlobMetadata> metadata) {
    var filterOperator = FilterString.newBuilder()
                                     .setOperator(FILTER_STRING_OPERATOR_EQUAL);
    var resultField = ResultField.newBuilder()
                                 .setResultRawField(ResultRawField.newBuilder()
                                                                  .setField(RESULT_RAW_ENUM_FIELD_RESULT_ID));
    var filterFieldBuilder = FilterField.newBuilder()
                                        .setField(resultField)
                                        .setFilterString(filterOperator);

    var resultFiltersBuilder = Filters.newBuilder();

    metadata.forEach(metadatum -> {
      filterFieldBuilder.setFilterString(FilterString.newBuilder().setValue(metadatum.id().toString()));
      resultFiltersBuilder.addOr(FiltersAnd.newBuilder().addAnd(filterFieldBuilder));
    });

    return EventSubscriptionRequest.newBuilder()
                                   .setResultsFilters(resultFiltersBuilder.build())
                                   .addReturnedEvents(EVENTS_ENUM_RESULT_STATUS_UPDATE)
                                   .addReturnedEvents(EVENTS_ENUM_NEW_RESULT)
                                   .setSessionId(sessionId.toString())
                                   .build();
  }
}
