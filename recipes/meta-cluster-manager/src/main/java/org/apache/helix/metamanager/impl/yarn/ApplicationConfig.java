package org.apache.helix.metamanager.impl.yarn;

public class ApplicationConfig {
	final String clusterAddress;
	final String clusterName;
	final String metadataAddress;
	final String providerName;

	public ApplicationConfig(String clusterAddress, String clusterName,
			String metadataAddress, String providerName) {
		this.clusterAddress = clusterAddress;
		this.clusterName = clusterName;
		this.metadataAddress = metadataAddress;
		this.providerName = providerName;
	}

	public String getClusterAddress() {
		return clusterAddress;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getMetadataAddress() {
		return metadataAddress;
	}

	public String getProviderName() {
		return providerName;
	}
}
