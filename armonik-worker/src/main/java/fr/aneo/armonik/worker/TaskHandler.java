package fr.aneo.armonik.worker;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;

public class TaskHandler {
  private final AgentFutureStub agentStub;

  public TaskHandler(AgentFutureStub agentStub) {
    this.agentStub = agentStub;
  }
}
