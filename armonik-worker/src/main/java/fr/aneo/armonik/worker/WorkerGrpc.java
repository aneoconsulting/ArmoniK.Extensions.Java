package fr.aneo.armonik.worker;

import fr.aneo.armonik.api.grpc.v1.worker.WorkerGrpc.WorkerImplBase;

public class WorkerGrpc extends WorkerImplBase {
  private final TaskProcessor taskProcessor;

  public WorkerGrpc(TaskProcessor taskProcessor) {
    this.taskProcessor = taskProcessor;
  }
}
