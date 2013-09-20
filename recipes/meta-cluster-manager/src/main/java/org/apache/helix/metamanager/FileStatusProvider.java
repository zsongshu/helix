package org.apache.helix.metamanager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class FileStatusProvider implements ClusterStatusProvider {

	final File file;
	
	public FileStatusProvider(File file) {
		this.file = file;
	}

	@Override
	public int getTargetContainerCount(String containerType) throws FileNotFoundException, IOException, IllegalArgumentException {
		Properties properties = new Properties();
		properties.load(new FileReader(file));
		if(!properties.contains(containerType))
			throw new IllegalArgumentException(String.format("container type '%s' not found in '%s'", containerType, file.getCanonicalPath()));
		return Integer.parseInt((String)properties.get(containerType));
	}

}
