package example;

import io.vertx.core.AbstractVerticle;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisCluster;
import util.RedisManager;
import util.Runner;

/**
 * Created by caijt on 2017/1/25.
 */
public class RedisManagerExample extends AbstractVerticle {

    public static void main(String[] args) {
        Runner.run(RedisManagerExample.class);
    }

    public void start() {
        RedisManager redisManager = new RedisManager(vertx);

        RedisClient client = redisManager.getRedisClient("redis-standalone");
        client.get("hello", ar -> {
            System.out.println("standalone:" + ar.result());
        });

        RedisCluster cluster = redisManager.getRedisCluster("redis-cluster");
        cluster.get("hello", ar -> {
            System.out.println("cluster:" + ar.result());
        });
    }
}
