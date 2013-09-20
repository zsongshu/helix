package org.apache.helix.metamanager.cluster;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.metamanager.ClusterStatusProvider;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

public class RedisTargetProvider implements ClusterStatusProvider {

    static final Logger        log               = Logger.getLogger(RedisTargetProvider.class);

    public static final String BENCHMARK_COMMAND = "redis-benchmark";
    public static final String BENCHMARK_TESTS   = "GET,SET";

    public static final String DEFAULT_RECORDS   = "100000";
    public static final String DEFAULT_CLIENTS   = "20";
    public static final String DEFAULT_REQUESTS  = "100000";
    public static final String DEFAULT_TIMEOUT   = "8000";
    public static final String DEFAULT_INTERVAL  = "10000";

    ZkClient                   zookeeper;

    final String               address;
    final String               root;

    final int                  records;
    final int                  clients;
    final int                  requests;
    final int                  timeout;
    final int                  interval;

    int                        targetTpsGet;
    int                        targetTpsSet;
    int                        targetCount       = 1;

    ScheduledExecutorService   executor;

    public RedisTargetProvider(Properties properties) {
        address = properties.getProperty("address");
        root = properties.getProperty("root");
        targetTpsGet = Integer.valueOf(properties.getProperty("tps.get", "0"));
        targetTpsSet = Integer.valueOf(properties.getProperty("tps.set", "0"));
        records = Integer.valueOf(properties.getProperty("records", DEFAULT_RECORDS));
        clients = Integer.valueOf(properties.getProperty("clients", DEFAULT_CLIENTS));
        requests = Integer.valueOf(properties.getProperty("requests", DEFAULT_REQUESTS));
        timeout = Integer.valueOf(properties.getProperty("timeout", DEFAULT_TIMEOUT));
        interval = Integer.valueOf(properties.getProperty("interval", DEFAULT_INTERVAL));
    }

    public void startService() {
        log.debug("starting redis status service");
        zookeeper = new ZkClient(address);
        zookeeper.createPersistent("/" + root, true);

        // TODO not concurrency-safe, should not matter though
        if (!zookeeper.exists("/" + root + "/target.get")) {
            try {
                zookeeper.createPersistent("/" + root + "/target.get", String.valueOf(targetTpsGet));
            } catch (Exception ignore) {
            }
        }
        if (!zookeeper.exists("/" + root + "/target.set")) {
            try {
                zookeeper.createPersistent("/" + root + "/target.set", String.valueOf(targetTpsSet));
            } catch (Exception ignore) {
            }
        }

        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new RedisBenchmarkRunnable(), 0, interval, TimeUnit.MILLISECONDS);
    }

    public void stopService() {
        log.debug("stopping redis status service");
        if (executor != null) {
            executor.shutdownNow();
            while (!executor.isTerminated()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            executor = null;
        }
        if (zookeeper != null) {
            zookeeper.close();
            zookeeper = null;
        }
    }

    @Override
    public int getTargetContainerCount(String containerType) throws Exception {
        return targetCount;
    }

    private class RedisBenchmarkRunnable implements Runnable {
        ExecutorService executor = Executors.newCachedThreadPool();
        RedisResult     aggregateResult;

        @Override
        public void run() {
            log.debug("running redis benchmark");

            aggregateResult = new RedisResult(0);
            Collection<Future<RedisResult>> futures = new ArrayList<Future<RedisResult>>();

            try {
                Collection<RedisTarget> targets = getTargets();

                // start benchmark
                for (RedisTarget target : targets) {
                    log.debug(String.format("submitting target '%s'", target));
                    Future<RedisResult> future = executor.submit(new RedisCallable(target));
                    futures.add(future);
                }

                // aggregate results
                try {
                    log.debug("waiting for results");

                    long limit = System.currentTimeMillis() + timeout;
                    for (Future<RedisResult> future : futures) {
                        try {
                            RedisResult result = future.get(limit - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                            log.debug(String.format("got result '%s'", result));
                            aggregate(result);
                        } catch (Exception e) {
                            log.warn(String.format("failed to get result"));
                            future.cancel(true);
                        }
                    }
                } catch (Exception e) {
                    log.error("Error running redis benchmark", e);

                    for (Future<RedisResult> future : futures) {
                        future.cancel(true);
                    }

                    return;
                }

                // compare to thresholds
                log.debug(String.format("aggregate result is '%s'", aggregateResult));

                // get target from zookeeper
                try {
                    targetTpsGet = Integer.valueOf(zookeeper.<String> readData("/" + root + "/target.get"));
                } catch (Exception ignore) {
                }
                try {
                    targetTpsSet = Integer.valueOf(zookeeper.<String> readData("/" + root + "/target.set"));
                } catch (Exception ignore) {
                }

                // calculate counts
                int targetCountGet = -1;
                if (aggregateResult.containsKey("GET")) {
                    double tpsTarget = targetTpsGet;
                    double tps = aggregateResult.get("GET");

                    targetCountGet = (int) Math.ceil(tpsTarget / tps * aggregateResult.serverCount);
                    log.debug(String.format("count.get=%d, tps.get=%f, target.get=%f", targetCountGet, tps, tpsTarget));
                }

                int targetCountSet = -1;
                if (aggregateResult.containsKey("SET")) {
                    double tpsTarget = targetTpsSet;
                    double tps = aggregateResult.get("SET");

                    targetCountSet = (int) Math.ceil(tpsTarget / tps * aggregateResult.serverCount);
                    log.debug(String.format("count.set=%d, tps.set=%f, target.set=%f", targetCountSet, tps, tpsTarget));
                }

                targetCount = Math.max(targetCountGet, targetCountSet);
                targetCount = Math.max(targetCount, 1);

                log.debug(String.format("target count is %d", targetCount));
                RedisTargetProvider.this.targetCount = targetCount;

            } catch (Exception e) {
                log.error("Error running redis benchmark", e);

                for (Future<RedisResult> future : futures) {
                    future.cancel(true);
                }
            }

        }

        Collection<RedisTarget> getTargets() {
            log.debug("fetching redis servers from zookeeper");
            Collection<RedisTarget> targets = new ArrayList<RedisTarget>();
            Collection<String> servers = zookeeper.getChildren("/" + root);

            servers.remove("target.get");
            servers.remove("target.set");

            for (String server : servers) {
                String hostname = zookeeper.readData("/" + root + "/" + server + "/hostname");
                int port = Integer.valueOf(zookeeper.<String> readData("/" + root + "/" + server + "/port"));

                targets.add(new RedisTarget(hostname, port));
            }

            log.debug(String.format("found %d servers: %s", targets.size(), targets));
            return targets;
        }

        void aggregate(RedisResult result) {
            RedisResult newResult = new RedisResult(aggregateResult.serverCount + result.serverCount);

            for (Entry<String, Double> entry : result.entrySet()) {
                double current = 0.0d;
                if (aggregateResult.containsKey(entry.getKey()))
                    current = aggregateResult.get(entry.getKey());

                current += entry.getValue();
                newResult.put(entry.getKey(), current);
            }

            aggregateResult = newResult;
        }
    }

    private static class RedisTarget {
        final String hostname;
        final int    port;

        public RedisTarget(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        @Override
        public String toString() {
            return String.format("%s:%d", hostname, port);
        }
    }

    private static class RedisResult extends HashMap<String, Double> {
        /**
         * 
         */
        private static final long serialVersionUID = 4599748807597500952L;

        final int                 serverCount;

        public RedisResult(int serverCount) {
            this.serverCount = serverCount;
        }

        @Override
        public String toString() {
            return String.format("[serverCount=%d %s]", serverCount, super.toString());
        }
    }

    private class RedisCallable implements Callable<RedisResult> {
        final RedisTarget target;

        public RedisCallable(RedisTarget target) {
            this.target = target;
        }

        @Override
        public RedisResult call() throws Exception {
            log.debug(String.format("executing benchmark for '%s'", target));

            ProcessBuilder builder = new ProcessBuilder();
            builder.command(BENCHMARK_COMMAND, "-h", target.hostname, "-p", String.valueOf(target.port), "-r", String.valueOf(records), "-n",
                    String.valueOf(requests), "-c", String.valueOf(clients), "-t", BENCHMARK_TESTS, "--csv");
            Process process = builder.start();

            log.debug(String.format("running '%s'", builder.command()));

            RedisResult result = new RedisResult(1);

            int retVal;
            try {
                retVal = process.waitFor();
            } catch (InterruptedException e) {
                process.destroy();
                return result;
            }

            Preconditions.checkState(retVal == 0, "Benchmark process returned %s", retVal);

            Pattern pattern = Pattern.compile("\"([A-Z0-9_]+).*\",\"([0-9\\.]+)\"");

            log.debug("parsing output");
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = pattern.matcher(line);

                if (!matcher.find())
                    continue;

                String key = matcher.group(1);
                Double value = Double.valueOf(matcher.group(2));

                result.put(key, value);
            }

            log.debug(String.format("benchmark for '%s' returned '%s'", target, result));

            return result;
        }
    }

}
