package org.apache.helix.api.model;

public final class ResourceId {
    private final String resourceName;

    public ResourceId(String resourceName) {
        this.resourceName = resourceName;
    }

    public String getResourceName() {
        return resourceName;
    }
}