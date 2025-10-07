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

import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawField;
import fr.aneo.armonik.api.grpc.v1.results.ResultsFilters;
import fr.aneo.armonik.client.model.BlobId;
import fr.aneo.armonik.client.model.SessionId;

import java.util.Set;

import static fr.aneo.armonik.api.grpc.v1.FiltersCommon.FilterString;
import static fr.aneo.armonik.api.grpc.v1.FiltersCommon.FilterStringOperator.FILTER_STRING_OPERATOR_EQUAL;
import static fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventSubscriptionRequest;
import static fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventsEnum.EVENTS_ENUM_NEW_RESULT;
import static fr.aneo.armonik.api.grpc.v1.events.EventsCommon.EventsEnum.EVENTS_ENUM_RESULT_STATUS_UPDATE;
import static fr.aneo.armonik.api.grpc.v1.results.ResultsFields.ResultRawEnumField.RESULT_RAW_ENUM_FIELD_RESULT_ID;

public final class EventMapper {

  private EventMapper() {
  }

  public static EventSubscriptionRequest createEventSubscriptionRequest(SessionId sessionId, Set<BlobId> blobIds) {
    var resultRawField = ResultField.newBuilder().setResultRawField(ResultRawField.newBuilder().setField(RESULT_RAW_ENUM_FIELD_RESULT_ID));
    var filterOperator = FilterString.newBuilder().setOperator(FILTER_STRING_OPERATOR_EQUAL);
    var filterFieldBuilder = ResultsFilters.FilterField.newBuilder()
                                                       .setField(resultRawField)
                                                       .setFilterString(filterOperator);

    var resultFiltersBuilder = ResultsFilters.Filters.newBuilder();
    blobIds.forEach(blobId -> {
      filterFieldBuilder.setFilterString(FilterString.newBuilder().setValue(blobId.asString()));
      resultFiltersBuilder.addOr(ResultsFilters.FiltersAnd.newBuilder().addAnd(filterFieldBuilder));
    });

    return EventSubscriptionRequest.newBuilder()
                                   .setResultsFilters(resultFiltersBuilder)
                                   .addReturnedEvents(EVENTS_ENUM_RESULT_STATUS_UPDATE)
                                   .addReturnedEvents(EVENTS_ENUM_NEW_RESULT)
                                   .setSessionId(sessionId.asString())
                                   .build();
  }
}
