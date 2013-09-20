package org.apache.helix.autoscale.impl.yarn;

/**
 * Container meta data for YARN-based containers. Reflect lifecycle of container
 * from requesting, to bootstrapping, active operation and shutdown. Read and
 * written by {@link YarnMasterProcess}, {@link YarnContainerProvider} and
 * {@link YarnContainerService}. Also read by {@link YarnStatusProvider}.
 * Typically stored in zookeeper
 * 
 */
class YarnContainerData {

	static enum ContainerState {
		ACQUIRE,
		CONNECTING,
		ACTIVE,
		TEARDOWN,
		FAILED,
		HALTED,
		FINALIZE
	}
	
    String                         id;
    ContainerState                 state;
    int                            yarnId;
    String                         owner;
    YarnContainerProcessProperties properties;

    public YarnContainerData() {
        // left blank
    }

    public YarnContainerData(String id, String owner, YarnContainerProcessProperties properties) {
        this.id = id;
        this.state = ContainerState.ACQUIRE;
        this.yarnId = -1;
        this.owner = owner;
        this.properties = properties;
    }

    public String getId() {
        return id;
    }

    public YarnContainerData setId(String id) {
        this.id = id;
        return this;
    }

    public ContainerState getState() {
        return state;
    }

    public YarnContainerData setState(ContainerState state) {
        this.state = state;
        return this;
    }

    public int getYarnId() {
        return yarnId;
    }

    public YarnContainerData setYarnId(int yarnId) {
        this.yarnId = yarnId;
        return this;
    }

    public String getOwner() {
        return owner;
    }

    public YarnContainerData setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public YarnContainerProcessProperties getProperties() {
        return properties;
    }

    public YarnContainerData setProperties(YarnContainerProcessProperties properties) {
        this.properties = properties;
        return this;
    }

}
