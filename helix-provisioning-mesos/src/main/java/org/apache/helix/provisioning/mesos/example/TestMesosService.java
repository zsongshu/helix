package org.apache.helix.provisioning.mesos.example;

import org.apache.helix.HelixConnection;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.provisioning.ServiceConfig;
import org.apache.helix.provisioning.participant.StatelessParticipantService;

public class TestMesosService extends StatelessParticipantService {

  public TestMesosService(HelixConnection connection, ClusterId clusterId, ParticipantId participantId,
      String serviceName) {
    super(connection, clusterId, participantId, serviceName);
  }

  @Override
  protected void init(ServiceConfig serviceConfig) {
  }

  @Override
  protected void goOnline() {
    System.err.println("going online");
  }

  @Override
  protected void goOffine() {
    System.err.println("going offline");
  }

}
