package org.apache.helix.autoscale.impl.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.autoscale.impl.yarn.YarnContainerData.ContainerState;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

/**
 * Utility for writing property files, transferring data via HDFS and
 * serializing {@link YarnContainerData} for zookeeper.
 * 
 */
class YarnUtils {

    static final Logger log                         = Logger.getLogger(YarnUtils.class);

    static final String YARN_MASTER_ARCHIVE_PATH    = "target/metamanager-assembly.tar.gz";
    static final String YARN_MASTER_PATH            = "master/metamanager/bin/yarn-master-process.sh";
    static final String YARN_MASTER_STAGING         = "master.tar.gz";
    static final String YARN_MASTER_DESTINATION     = "master";
    static final String YARN_MASTER_PROPERTIES      = "master.properties";
    static final String YARN_CONTAINER_ARCHIVE_PATH = "target/metamanager-assembly.tar.gz";
    static final String YARN_CONTAINER_STAGING      = "container.tar.gz";
    static final String YARN_CONTAINER_PATH         = "container/metamanager/bin/yarn-container-process.sh";
    static final String YARN_CONTAINER_DESTINATION  = "container";
    static final String YARN_CONTAINER_PROPERTIES   = "container.properties";

    static Gson         gson;
    static {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(ContainerState.class, new ContainerStateAdapter());
        builder.setPrettyPrinting();
        gson = builder.create();
    }

    public static String toJson(YarnContainerData meta) {
        return gson.toJson(meta);
    }

    public static YarnContainerData fromJson(String json) {
        return gson.fromJson(json, YarnContainerData.class);
    }

    public static Properties getPropertiesFromPath(String path) throws IOException {
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream(path)));
        return properties;
    }

    public static File writePropertiesToTemp(Properties properties) throws IOException {
        File tmpFile = File.createTempFile("provider", ".properties");
        Writer writer = Files.newWriter(tmpFile, Charset.defaultCharset());
        properties.store(writer, null);
        writer.flush();
        writer.close();
        return tmpFile;
    }

    public static Path copyToHdfs(String source, String dest, String namespace, Configuration conf) throws IOException {
        Path sourcePath = makeQualified(source);
        Path destPath = makeQualified(conf.get(FileSystem.FS_DEFAULT_NAME_KEY) + "/" + namespace + "/" + dest);
        log.debug(String.format("Copying '%s' to '%s'", sourcePath, destPath));

        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(false, true, sourcePath, destPath);
        fs.close();
        return destPath;
    }

    public static void destroyHdfsNamespace(String namespace, Configuration conf) throws IOException {
        Path path = makeQualified(conf.get(FileSystem.FS_DEFAULT_NAME_KEY) + "/" + namespace);
        log.debug(String.format("Deleting '%s'", path));

        FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
        fs.close();
    }

    public static LocalResource createHdfsResource(Path path, LocalResourceType type, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        URL url = ConverterUtils.getYarnUrlFromPath(path);

        FileStatus status = fs.getFileStatus(path);

        LocalResource resource = Records.newRecord(LocalResource.class);
        resource.setResource(url);
        resource.setSize(status.getLen());
        resource.setTimestamp(status.getModificationTime());
        resource.setType(type);
        resource.setVisibility(LocalResourceVisibility.APPLICATION);

        fs.close();

        return resource;
    }

    static Path makeQualified(String path) throws UnsupportedFileSystemException {
        return FileContext.getFileContext().makeQualified(new Path(path));
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

    static YarnContainerProcessProperties createContainerProcessProperties(Properties properties) {
        Preconditions.checkNotNull(properties);
        YarnContainerProcessProperties yarnProp = new YarnContainerProcessProperties();
        yarnProp.putAll(properties);
        return yarnProp;
    }

    static YarnContainerProviderProperties createContainerProviderProperties(Properties properties) {
        Preconditions.checkNotNull(properties);
        YarnContainerProviderProperties yarnProp = new YarnContainerProviderProperties();
        yarnProp.putAll(properties);
        return yarnProp;
    }

    static YarnMasterProperties createMasterProperties(Properties properties) {
        Preconditions.checkNotNull(properties);
        YarnMasterProperties yarnProp = new YarnMasterProperties();
        yarnProp.putAll(properties);
        return yarnProp;
    }

    private YarnUtils() {
        // left blank
    }

}
