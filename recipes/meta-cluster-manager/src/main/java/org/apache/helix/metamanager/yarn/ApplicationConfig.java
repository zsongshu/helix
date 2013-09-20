package org.apache.helix.metamanager.yarn;

public class ApplicationConfig {
	final String clusterAddress;
	final String clusterName;
	final String providerAddress;
	final String providerName;

	public ApplicationConfig(String clusterAddress, String clusterName,
			String providerAddress, String providerName) {
		this.clusterAddress = clusterAddress;
		this.clusterName = clusterName;
		this.providerAddress = providerAddress;
		this.providerName = providerName;
	}

	public String getClusterAddress() {
		return clusterAddress;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getProviderAddress() {
		return providerAddress;
	}

	public String getProviderName() {
		return providerName;
	}
}
