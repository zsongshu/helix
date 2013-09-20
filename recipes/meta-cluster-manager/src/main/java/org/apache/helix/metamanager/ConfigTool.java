package org.apache.helix.metamanager;

import org.apache.log4j.Logger;

public class ConfigTool {
	
	static final Logger log = Logger.getLogger(ConfigTool.class);
	
    public static final String SHELL_CONTAINER_PATH        = "target/metamanager-pkg/bin/shell-container-process.sh";
    public static final String SHELL_CONTAINER_PROPERTIES  = "container.properties";
    public static final String SHELL_CONTAINER_MARKER      = "active";

    public static final String YARN_MASTER_ARCHIVE_PATH    = "target/metamanager-assembly.tar.gz";
    public static final String YARN_MASTER_PATH            = "master/metamanager/bin/yarn-master-process.sh";
    public static final String YARN_MASTER_STAGING         = "master.tar.gz";
    public static final String YARN_MASTER_DESTINATION     = "master";
    public static final String YARN_MASTER_PROPERTIES      = "master.properties";
    public static final String YARN_CONTAINER_ARCHIVE_PATH = "target/metamanager-assembly.tar.gz";
    public static final String YARN_CONTAINER_STAGING      = "container.tar.gz";
    public static final String YARN_CONTAINER_PATH         = "container/metamanager/bin/yarn-container-process.sh";
    public static final String YARN_CONTAINER_DESTINATION  = "container";
    public static final String YARN_CONTAINER_PROPERTIES   = "container.properties";

    public static final long   CONTAINER_TIMEOUT           = 60000;

	static TargetProvider targetProvider;
	static StatusProvider statusProvider;
	
	private ConfigTool() {
		// left blank
	}
	
	public static TargetProvider getTargetProvider() {
		return targetProvider;
	}
	public static void setTargetProvider(TargetProvider targetProvider) {
		ConfigTool.targetProvider = targetProvider;
	}
	
	public static StatusProvider getStatusProvider() {
		return statusProvider;
	}
	public static void setStatusProvider(StatusProvider statusProvider) {
		ConfigTool.statusProvider = statusProvider;
	}
	
}
