package org.apache.helix.api.model;

import java.util.List;

public interface NodeInstance {
    public interface Builder {
        public void setName(String name);

        public void setEnabled(boolean enabled);

        public <T> void setProperty(PropertyLifetime lifetime, String name, T value);

        public void setHost(String host);

        public void setPort(int port);

        public void setTagList(List<String> tagList);

        public void setBlacklistedPartitions(List<PartitionId> partitionIds);
    }

    public String getName();

    public NodeInstanceState getNodeInstanceState();

    public boolean isEnabled();

    public <T> T getProperty(String name);

    public String getHost();

    public int getPort();

    public List<String> getTagList();

    public List<PartitionId> getBlacklistedPartitions();
}