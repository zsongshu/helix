package org.apache.helix.provisioning.mesos;

import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.provisioner.ProvisionerRef;
import org.apache.helix.controller.serializer.DefaultStringSerializer;
import org.apache.helix.controller.serializer.StringSerializer;
import org.codehaus.jackson.annotate.JsonProperty;

public class MesosProvisionerConfig implements ProvisionerConfig {
  private ResourceId _resourceId;
  private ProvisionerRef _provisionerRef;
  private Class<? extends StringSerializer> _serializerClass;
  private String _className;
  private String _masterAddress;
  private Integer _numContainers;

  public MesosProvisionerConfig(@JsonProperty("resourceId") ResourceId resourceId) {
    _resourceId = resourceId;
    _provisionerRef = ProvisionerRef.from(MesosProvisioner.class);
    _serializerClass = DefaultStringSerializer.class;
  }

  public void setAppClassName(String appClassName) {
    _className = appClassName;
  }

  public String getAppClassName() {
    return _className;
  }

  public void setMasterAddress(String masterAddress) {
    _masterAddress = masterAddress;
  }

  public String getMasterAddress() {
    return _masterAddress;
  }

  public void setNumContainers(int numContainers) {
    _numContainers = numContainers;
  }

  public Integer getNumContainers() {
    return _numContainers;
  }

  @Override
  public ResourceId getResourceId() {
    return _resourceId;
  }

  @Override
  public ProvisionerRef getProvisionerRef() {
    return _provisionerRef;
  }

  public void setProvisionerRef(ProvisionerRef provisionerRef) {
    _provisionerRef = provisionerRef;
  }

  @Override
  public Class<? extends StringSerializer> getSerializerClass() {
    return _serializerClass;
  }

  public void setSerializerClass(Class<? extends StringSerializer> serializerClass) {
    _serializerClass = serializerClass;
  }
}
