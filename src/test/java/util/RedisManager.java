package util;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.*;

import java.util.*;

/**
 * Created by Caijt on 2017/1/23.
 */
public class RedisManager {

    private static final String CONFIG_FILE = "redis-config.json";
    private static final String REDIS_HOST = "host";
    private static final String REDIS_PORT = "port";
    private static final String REDIS_DB_NUMBER = "db";
    private static final String REDIS_CONFIG = "config";

    private static final int DEFAULT_DB_SELECT = 0;

    private JsonObject redisConfig;
    private Vertx vertx;

    private final Map<String, Object> pool = new HashMap<>();
    private final Random random = new Random();

    public RedisManager(Vertx vertx) {
        this.vertx = vertx;
        redisConfig = new JsonObject(vertx.fileSystem().readFileBlocking(CONFIG_FILE).toString());
    }

    /**
     * for standalone
     */
    public RedisClient getRedisClient(String name) {
        RedisClient redisClient = (RedisClient) pool.get(name);
        if(redisClient == null) {
            JsonObject config = redisConfig.getJsonObject(name);
            redisClient = createRedisClient(config.getJsonObject(REDIS_CONFIG));
            pool.put(name, redisClient);
        }
        return redisClient;
    }

    /**
     * for redis list
     */
    public RedisClient getRedisClient(String name, int index) {
        List redisClientList = (List) pool.get(name);
        if(redisClientList == null) {
            JsonObject config = redisConfig.getJsonObject(name);
            createRedisClientList(name, config.getJsonArray(REDIS_CONFIG));
            redisClientList = (List) pool.get(name);
        }
        return (RedisClient) redisClientList.get(index);
    }

    /**
     * for redis list
     */
    public RedisClient getRedisClientByRandom(String name) {
        List redisClientList = (List) pool.get(name);
        if(redisClientList == null) {
            JsonObject config = redisConfig.getJsonObject(name);
            createRedisClientList(name, config.getJsonArray(REDIS_CONFIG));
            redisClientList = (List) pool.get(name);
        }
        int index = random.nextInt(redisClientList.size());
        return (RedisClient) redisClientList.get(index);
    }

    /**
     * for redis cluster
     */
    public RedisCluster getRedisCluster(String name) {
        RedisCluster redisCluster = (RedisCluster) pool.get(name);
        if(redisCluster == null) {
            JsonObject config = redisConfig.getJsonObject(name);
            createRedisCluster(name, config.getJsonArray(REDIS_CONFIG));
            redisCluster = (RedisCluster) pool.get(name);
        }
        return redisCluster;
    }

    private void createRedisCluster(String name, JsonArray config) {
        // get cluster config
        RedisClusterOptions options = new RedisClusterOptions();
        for(int i = 0; i < config.size(); i++) {
            JsonObject hostAndPort = config.getJsonObject(i);
            options.addNode(new HostAndPort(hostAndPort.getString(REDIS_HOST), hostAndPort.getInteger(REDIS_PORT)));
        }
        // create redis cluster
        RedisCluster redisCluster = RedisCluster.create(vertx, options);
        // add to redis pool
        pool.put(name, redisCluster);
    }

    private void createRedisClientList(String name, JsonArray config) {
        List<RedisClient> redisClientList = new ArrayList<>();
        for(int i = 0; i < config.size(); i++) {
            JsonObject option = config.getJsonObject(i);
            RedisClient redisClient = createRedisClient(option);
            redisClientList.add(redisClient);
        }
        // add to redis pool
        pool.put(name, redisClientList);
    }

    private RedisClient createRedisClient(JsonObject config) {
        String host = config.getString(REDIS_HOST);
        Integer port = config.getInteger(REDIS_PORT);
        Integer db = config.getInteger(REDIS_DB_NUMBER);
        return RedisClient.create(vertx,
                new RedisOptions().setHost(host).setPort(port).setSelect(db == null ? DEFAULT_DB_SELECT : db));
    }
}
