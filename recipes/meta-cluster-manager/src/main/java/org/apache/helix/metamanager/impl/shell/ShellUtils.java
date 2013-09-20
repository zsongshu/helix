package org.apache.helix.metamanager.impl.shell;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Utility for creating and destroying temporary marker files for shell-based
 * containers.
 * 
 */
class ShellUtils {

    static final Logger log                        = Logger.getLogger(ShellUtils.class);

    static final String SHELL_CONTAINER_PATH       = "target/metamanager-pkg/bin/shell-container-process.sh";
    static final String SHELL_CONTAINER_PROPERTIES = "container.properties";
    static final String SHELL_CONTAINER_MARKER     = "active";

    private ShellUtils() {
        // left blank
    }

    public static boolean hasMarker(File processDir) {
        try {
            log.debug(String.format("checking for marker file '%s'", getMarkerFile(processDir)));
            if (getMarkerFile(processDir).exists())
                return true;
        } catch (IOException e) {
            // ignore
        }
        return false;
    }

    public static void createMarker(File processDir) throws IOException {
        log.debug(String.format("creating marker file '%s'", getMarkerFile(processDir)));
        getMarkerFile(processDir).createNewFile();
    }

    public static void destroyMarker(File processDir) {
        try {
            log.debug(String.format("destroying marker file '%s'", getMarkerFile(processDir)));
            getMarkerFile(processDir).delete();
        } catch (IOException e) {
            // ignore
        }
    }

    public static File getMarkerFile(File processDir) throws IOException {
        return new File(processDir.getCanonicalPath() + File.separatorChar + SHELL_CONTAINER_MARKER);
    }

}
