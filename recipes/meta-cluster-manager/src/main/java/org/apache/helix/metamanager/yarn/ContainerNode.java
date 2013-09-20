package org.apache.helix.metamanager.yarn;

import java.io.Serializable;

import org.apache.hadoop.yarn.api.records.ContainerId;


class ContainerNode implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2578978959080378923L;

	static enum ContainerState {
		ACQUIRE,
		CONNECT,
		READY,
		STARTING,
		RUNNING,
		STOPPING,
		TEARDOWN,
		FINALIZE
	}
	
	final String id;
	final ContainerState state;
	final ContainerId yarnId;
	
	final String zkAddress;
	final String clusterName;
	final String command;

	public ContainerNode(String id, String zkAddress, String clusterName, String command) {
		this.id = id;
		this.state = ContainerState.ACQUIRE;
		this.yarnId = null;
		this.zkAddress = zkAddress;
		this.clusterName = clusterName;
		this.command = command;
	}
	
	public ContainerNode(ContainerNode node, ContainerState state) {
		this.id = node.id;
		this.state = state;
		this.yarnId = node.yarnId;
		this.zkAddress = node.zkAddress;
		this.clusterName = node.clusterName;
		this.command = node.command;
	}
	
	public ContainerNode(ContainerNode node, ContainerState state, ContainerId yarnId) {
		this.id = node.id;
		this.state = state;
		this.yarnId = yarnId;
		this.zkAddress = node.zkAddress;
		this.clusterName = node.clusterName;
		this.command = node.command;
	}
	
}

