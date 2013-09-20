package org.apache.helix.metamanager.yarn;


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
	String command;
	String owner;

	public ContainerMetadata() {
		// left blank
	}
	
	public ContainerMetadata(String id, String command, String owner) {
		this.id = id;
		this.state = ContainerState.ACQUIRE;
		this.yarnId = -1;
		this.command = command;
		this.owner = owner;
	}
	
	public ContainerMetadata(ContainerMetadata node, ContainerState state) {
		this.id = node.id;
		this.state = state;
		this.yarnId = node.yarnId;
		this.command = node.command;
		this.owner = node.owner;
	}
	
	public ContainerMetadata(ContainerMetadata node, ContainerState state, int yarnId) {
		this.id = node.id;
		this.state = state;
		this.yarnId = yarnId;
		this.command = node.command;
		this.owner = node.owner;
	}
}

