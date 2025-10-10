package fr.aneo.armonik.worker;

import fr.aneo.armonik.api.grpc.v1.worker.WorkerCommon;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;

@FunctionalInterface
public interface TaskHandlerFactory {
  TaskHandler create(AgentFutureStub agentStub, WorkerCommon.ProcessRequest request);
}
