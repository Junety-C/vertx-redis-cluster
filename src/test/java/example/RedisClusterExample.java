package example;

import io.vertx.core.AbstractVerticle;
import io.vertx.redis.*;
import io.vertx.redis.HostAndPort;
import util.Runner;

/**
 * Created by Caijt on 2017/1/20.
 */
public class RedisClusterExample extends AbstractVerticle {

    public static void main(String[] args) {
        Runner.run(RedisClusterExample.class);
    }

    public void start() {
        RedisClusterOptions options = new RedisClusterOptions()
                .addNode(new HostAndPort("127.0.0.1", 17002))
                .addNode(new HostAndPort("127.0.0.1", 17003))
                .addNode(new HostAndPort("127.0.0.1", 17001));

        RedisCluster redisCluster = RedisCluster.create(vertx, options);

        redisCluster.get("6212", ar -> {
            System.out.println("result:" + ar.result());
            System.out.println("result:" + ar.cause());
        });
    }
}
