package fr.aneo.armonik.worker;

import fr.aneo.armonik.api.grpc.v1.Objects.Output.Error;
import fr.aneo.armonik.api.grpc.v1.worker.WorkerGrpc.WorkerImplBase;
import io.grpc.stub.StreamObserver;

import static fr.aneo.armonik.api.grpc.v1.Objects.Empty;
import static fr.aneo.armonik.api.grpc.v1.Objects.Output;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.*;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessReply;
import static fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon.ProcessRequest;

public class WorkerGrpc extends WorkerImplBase {
  private final AgentFutureStub agentStub;
  private final TaskProcessor taskProcessor;

  public WorkerGrpc(AgentFutureStub agentStub, TaskProcessor taskProcessor) {
    this.agentStub = agentStub;
    this.taskProcessor = taskProcessor;
  }

  @Override
  public void process(ProcessRequest request, StreamObserver<ProcessReply> responseObserver) {
    var taskHandler = new TaskHandler(agentStub);
    try {
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
      responseObserver.onCompleted();
    }
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
