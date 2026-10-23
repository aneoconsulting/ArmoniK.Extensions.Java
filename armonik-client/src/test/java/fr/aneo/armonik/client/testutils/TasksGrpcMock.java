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
package fr.aneo.armonik.client.testutils;

import fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksResponse.TaskInfo;
import fr.aneo.armonik.api.grpc.v1.tasks.TasksGrpc;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksRequest;
import static fr.aneo.armonik.api.grpc.v1.tasks.TasksCommon.SubmitTasksResponse;

public class TasksGrpcMock extends TasksGrpc.TasksImplBase {

  public SubmitTasksRequest submittedTasksRequest;
  public final List<SubmitTasksRequest> submitTasksRequests = new CopyOnWriteArrayList<>();
  public volatile RuntimeException submissionError;

  @Override
  public void submitTasks(SubmitTasksRequest request, StreamObserver<SubmitTasksResponse> responseObserver) {
    SubmitTasksResponse.Builder builder = SubmitTasksResponse.newBuilder();
    submittedTasksRequest = request;
    submitTasksRequests.add(request);
    if (submissionError != null) {
      responseObserver.onError(submissionError);
      return;
    }
    IntStream.range(0, request.getTaskCreationsCount()).forEach(index ->
      builder.addTaskInfos(TaskInfo.newBuilder()
                                   .setTaskId("task-" + request.getTaskCreations(index).getPayloadId())
                                   .setPayloadId(request.getTaskCreations(index).getPayloadId())
                                   .addAllDataDependencies(request.getTaskCreations(index).getDataDependenciesList())
                                   .addAllExpectedOutputIds(request.getTaskCreations(index).getExpectedOutputKeysList()))
    );
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }
}
