package org.apache.helix.metamanager.impl.yarn;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.metamanager.impl.yarn.ContainerMetadata.ContainerState;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

public class Utils {
	
	static final Logger log = Logger.getLogger(Utils.class);
	
	static Gson gson;
	static {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(ContainerState.class, new ContainerStateAdapter());
		builder.setPrettyPrinting();
		gson = builder.create();
	}
	
	static Map<String, LocalResource>  dummyResources = createDummyResources();
	
	static String toJson(ContainerMetadata meta) {
		return gson.toJson(meta);
	}
	
	static ContainerMetadata fromJson(String json) {
		return gson.fromJson(json, ContainerMetadata.class);
	}
	
	static Map<String, LocalResource> getDummyResources() {
		return dummyResources;
	}

	private static Map<String, LocalResource> createDummyResources() {
		File dummy = new File("/tmp/dummy");
		
		if(!dummy.exists()) {
	    	try {
	    		dummy.createNewFile();
	    	} catch(Exception e) {
	    		log.error("could not create dummy file", e);
	    		System.exit(1);
	    	}
		}
	    
	    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
	    Path path = new Path(dummy.toURI());
	    LocalResource localResource = Records.newRecord(LocalResource.class);
	    localResource.setType(LocalResourceType.FILE);
	    localResource.setVisibility(LocalResourceVisibility.APPLICATION);          
	    localResource.setResource(ConverterUtils.getYarnUrlFromPath(path)); 
	    localResource.setTimestamp(dummy.lastModified());
	    localResource.setSize(dummy.length());
	    localResources.put("dummy", localResource);
		return localResources;
	}
	
	static class ContainerStateAdapter extends TypeAdapter<ContainerState> {
		@Override
		public ContainerState read(JsonReader reader) throws IOException {
			if (reader.peek() == JsonToken.NULL) {
				reader.nextNull();
				return null;
			}
			return ContainerState.valueOf(reader.nextString());
		}

		@Override
		public void write(JsonWriter writer, ContainerState value) throws IOException {
			if (value == null) {
				writer.nullValue();
				return;
			}
			writer.value(value.name());
		}
	}
	
}
