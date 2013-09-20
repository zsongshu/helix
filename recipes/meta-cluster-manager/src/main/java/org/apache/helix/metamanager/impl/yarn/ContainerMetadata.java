package org.apache.helix.metamanager.impl.yarn;


class ContainerMetadata {

	static enum ContainerState {
		ACQUIRE,
		CONNECTING,
		ACTIVE,
		TEARDOWN,
		FAILED,
		HALTED,
		FINALIZE
	}
	
	String id;
	ContainerState state;
	int yarnId;
	String owner;
	YarnContainerProcessProperties properties;

	public ContainerMetadata() {
		// left blank
	}
	
	public ContainerMetadata(String id, String owner, YarnContainerProcessProperties properties) {
		this.id = id;
		this.state = ContainerState.ACQUIRE;
		this.yarnId = -1;
		this.owner = owner;
		this.properties = properties;
	}

	public String getId() {
		return id;
	}

	public ContainerMetadata setId(String id) {
		this.id = id;
		return this;
	}

	public ContainerState getState() {
		return state;
	}

	public ContainerMetadata setState(ContainerState state) {
		this.state = state;
		return this;
	}

	public int getYarnId() {
		return yarnId;
	}

	public ContainerMetadata setYarnId(int yarnId) {
		this.yarnId = yarnId;
		return this;
	}

	public String getOwner() {
		return owner;
	}

	public ContainerMetadata setOwner(String owner) {
		this.owner = owner;
		return this;
	}

	public YarnContainerProcessProperties getProperties() {
		return properties;
	}

	public ContainerMetadata setProperties(YarnContainerProcessProperties properties) {
		this.properties = properties;
		return this;
	}
	
}

