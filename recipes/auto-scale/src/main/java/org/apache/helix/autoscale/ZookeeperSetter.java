package org.apache.helix.autoscale;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

/**
 * Utility for setting String values in the embedded zookeeper service.
 * (Program entry point)
 * 
 */
public class ZookeeperSetter {

    static Logger log = Logger.getLogger(ZookeeperSetter.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        String address = args[0];
        String path = args[1];
        String value = args[2];

        log.info(String.format("Setting %s:%s to '%s'", address, path, value));

        ZkClient client = new ZkClient(address);
        client.createPersistent(path, true);
        client.writeData(path, value);
    }

}
