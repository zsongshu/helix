package org.apache.helix.api.model;

public final class PartitionId {
    private final ResourceId resourceId;

    private final String partitionName;

    public PartitionId(ResourceId resourceId, String partitionName) {
        this.resourceId = resourceId;
        this.partitionName = partitionName;
    }

    public ResourceId getResourceId() {
        return resourceId;
    }

    public String getPartitionName() {
        return partitionName;
    }
}