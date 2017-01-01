import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
import spark.route.RouteOverview;

import static spark.Spark.exception;
import static spark.Spark.get;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static Integer num = 0;

    public static void main(String... args) throws Exception {

        Spark.port(8080);

        RouteOverview.enableRouteOverview();

        /*
         * ZOOKEEPER
         */

        CuratorFramework zookeeperClient = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        zookeeperClient.start();

        zookeeperClient.checkExists().creatingParentContainersIfNeeded().forPath("/locks/GET");

        InterProcessMutex mutex = new InterProcessMutex(zookeeperClient, "/locks/GET");

        /*
         * HAZELCAST
         */

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("localhost:5701");
        HazelcastInstance hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);

        Lock hcLock = hazelcastClient.getLock("my-distributed-lock");

        /*
         * REST API
         */

        exception(Exception.class, (e, request, response) -> {

            LOGGER.error(e.getMessage());
        });

        get("/test/hazelcast-lock", (req, res) -> {

            hcLock.lock();

            try {
                res.status(test());
            } finally {
                hcLock.unlock();
            }

            return "";
        });

        get("/test/zookeeper-lock", (req, res) -> {

            mutex.acquire();

            try {
                res.status(test());
            } finally {
                mutex.release();
            }

            return "";
        });

        get("/test/no-lock", (req, res) -> {

            res.status(test());

            return "";
        });
    }

    private static Integer test() {

        Integer result = 200;

        UUID uuid = UUID.randomUUID();

        LOGGER.info("[{}] begin: {}", uuid, num);

        num++;

        LOGGER.info("[{}] middle: {}", uuid, num);

        if (num > 1) {
            // with working locking, the execution should never get here
            LOGGER.error("[{}] middle: {}", uuid, num);
            result = 500;
        }

        num--;

        LOGGER.info("[{}] end: {}", uuid, num);

        return result;
    }
}
