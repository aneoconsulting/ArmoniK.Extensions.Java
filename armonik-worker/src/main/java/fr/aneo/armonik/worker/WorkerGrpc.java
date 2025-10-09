package fr.aneo.armonik.worker;

import fr.aneo.armonik.api.grpc.v1.Objects.Output.Error;
import fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus;
import fr.aneo.armonik.api.grpc.v1.worker.WorkerGrpc.WorkerImplBase;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicReference;

import static fr.aneo.armonik.api.grpc.v1.Objects.Empty;
import static fr.aneo.armonik.api.grpc.v1.Objects.Output;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.*;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus.NOT_SERVING;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.HealthCheckReply.ServingStatus.SERVING;

public class WorkerGrpc extends WorkerImplBase {
  private final AgentFutureStub agentStub;
  private final TaskProcessor taskProcessor;
  public final AtomicReference<ServingStatus> servingStatus;

  public WorkerGrpc(AgentFutureStub agentStub, TaskProcessor taskProcessor) {
    this.agentStub = agentStub;
    this.taskProcessor = taskProcessor;
    this.servingStatus = new AtomicReference<>(SERVING);
  }

  @Override
  public void process(ProcessRequest request, StreamObserver<ProcessReply> responseObserver) {
    servingStatus.set(NOT_SERVING);
    try {
      var taskHandler = new TaskHandler(agentStub);
      var outcome = taskProcessor.processTask(taskHandler);
      responseObserver.onNext(ProcessReply.newBuilder()
                                          .setOutput(toOutput(outcome))
                                          .build());
    } catch (Exception exception) {
      var message = exception.getMessage() != null ? exception.getMessage() : exception.toString();
      responseObserver.onNext(ProcessReply.newBuilder()
                                          .setOutput(toOutput(new TaskOutcome.Error(message)))
                                          .build());
    } finally {
      servingStatus.set(SERVING);
      responseObserver.onCompleted();
    }
  }

@Override
  public void healthCheck(Empty request, StreamObserver<HealthCheckReply> responseObserver) {
    responseObserver.onNext(HealthCheckReply.newBuilder()
                                            .setStatus(servingStatus.get())
                                            .build());
    responseObserver.onCompleted();
  }

  private Output toOutput(TaskOutcome outcome) {
    var outputBuilder = Output.newBuilder();
    if (outcome instanceof TaskOutcome.Success) {
      outputBuilder.setOk(Empty.newBuilder().build());
    } else if (outcome instanceof TaskOutcome.Error error) {
      outputBuilder.setError(Error.newBuilder().setDetails(error.message()).build());
    }
    return outputBuilder.build();
  }
}
