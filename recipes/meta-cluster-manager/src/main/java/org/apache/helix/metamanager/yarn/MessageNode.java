package org.apache.helix.metamanager.yarn;


class MessageNode {
	static enum MessageType {
		CREATE,
		START,
		STOP,
		DESTROY
	}
	
	final String id;
	final MessageType type;

	public MessageNode(String id, MessageType type) {
		this.id = id;
		this.type = type;
	}
}

