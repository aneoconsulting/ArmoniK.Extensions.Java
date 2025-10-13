package fr.aneo.armonik.worker;

import fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataRequest.ResultIdentifier;

import static fr.aneo.armonik.api.grpc.v1.agent.AgentCommon.NotifyResultDataRequest;
import static fr.aneo.armonik.api.grpc.v1.agent.AgentGrpc.AgentFutureStub;

@FunctionalInterface
interface BlobListener {
  void onBlobReady(BlobId blobId);

  final class AgentNotifier implements BlobListener {
    private final AgentFutureStub agentStub;
    private final String sessionId;
    private final String communicationToken;

    AgentNotifier(AgentFutureStub agentStub, String sessionId, String communicationToken) {
      this.agentStub = agentStub;
      this.sessionId = sessionId;
      this.communicationToken = communicationToken;
    }

    @Override
    public void onBlobReady(BlobId blobId) {
      agentStub.notifyResultData(NotifyResultDataRequest.newBuilder()
                                                        .setCommunicationToken(communicationToken)
                                                        .addIds(ResultIdentifier.newBuilder()
                                                                                .setSessionId(sessionId)
                                                                                .setResultId(blobId.asString())
                                                                                .build())
                                                        .build());
    }
  }
}
