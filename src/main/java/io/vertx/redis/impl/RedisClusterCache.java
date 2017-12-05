package io.vertx.redis.impl;

import io.vertx.core.*;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisClusterOptions;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.HostAndPort;

import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by Caijt on 2017/1/23.
 */
class RedisClusterCache {

    private final Vertx vertx;
    private final Context context;
    private final Map<String, RedisConnection> nodesCache;
    private final Map<Integer, RedisConnection> slotsCache;

    RedisClusterCache(Vertx vertx) {
        this.vertx = vertx;
        this.context = this.getContext(vertx);
        this.nodesCache = new HashMap<>();
        this.slotsCache = new HashMap<>(16384);
    }

    RedisConnection getRedis(int slot) {
        return slotsCache.get(slot);
    }

    RedisConnection getRedis(String nodeKey) {
        return nodesCache.get(nodeKey);
    }

    RedisConnection getRedis(HostAndPort hostAndPort) {
        return nodesCache.get(getNodeKey(hostAndPort));
    }

    void discoverClusterNodesAndSlots(RedisClient client, RedisClusterOptions config, Future<Void> future) {
        this.nodesCache.clear();
        discoverClusterSlots(client, config, future);
    }

    void discoverClusterSlots(RedisClient client, RedisClusterOptions config, Future<Void> future) {
        this.slotsCache.clear();
        client.clusterSlots(ar -> {
            if(ar.succeeded()) {
                JsonArray slotInfoArray = ar.result();
                for(int i = 0; i < slotInfoArray.size(); i++) {
                    JsonArray slotInfo = slotInfoArray.getJsonArray(i);
                    if(slotInfo.size() <= 2) continue;
                    JsonArray hostInfo = slotInfo.getJsonArray(2);
                    if(hostInfo.size() < 2) continue;
                    HostAndPort hostAndPort = generateHostAndPort(hostInfo);
                    RedisOptions options = config.cloneRedisOptions();
                    options.setHost(hostAndPort.getHost());
                    options.setPort(hostAndPort.getPort());
                    assignSlotsToNode(slotInfo, options);
                }
                future.complete();
            } else {
                future.fail(ar.cause());
            }
        });
    }

    void discoverClusterSlots(RedisConnection connection, RedisClusterOptions config, Future<Void> future) {
        this.slotsCache.clear();
        final Command<JsonArray> cmd = new Command<>(this.context, RedisCommand.CLUSTER_SLOTS, null,
                Charset.defaultCharset(), ResponseTransform.NONE, JsonArray.class).handler(ar -> {
            if(ar.succeeded()) {
                JsonArray slotInfoArray = ar.result();
                for(int i = 0; i < slotInfoArray.size(); i++) {
                    JsonArray slotInfo = slotInfoArray.getJsonArray(i);
                    if(slotInfo.size() <= 2) continue;
                    JsonArray hostInfo = slotInfo.getJsonArray(2);
                    if(hostInfo.size() < 2) continue;
                    HostAndPort hostAndPort = generateHostAndPort(hostInfo);
                    RedisOptions options = config.cloneRedisOptions();
                    options.setHost(hostAndPort.getHost());
                    options.setPort(hostAndPort.getPort());
                    assignSlotsToNode(slotInfo, options);
                }
                future.complete();
            } else {
                future.fail(ar.cause());
            }
        });
        connection.send(cmd);
    }

    void renewClusterSlots(RedisClusterOptions config, Handler<AsyncResult<Void>> handler) {
        renewClusterSlots(getShuffledNodesPool(), 0, config, handler);
    }

    private void renewClusterSlots(List<RedisConnection> connections, int index, RedisClusterOptions config, Handler<AsyncResult<Void>> handler) {
        if(index < connections.size()) {
            RedisConnection connection = connections.get(index);
            Future<Void> future = Future.future();
            this.discoverClusterSlots(connection, config, future.setHandler(ar -> {
                // if current redis connection error, try the next one
                if(ar.failed()) {
                    renewClusterSlots(connections, index + 1, config, handler);
                } else {
                    runOnContext(v -> handler.handle(Future.succeededFuture()));
                }
            }));
        } else {
            runOnContext(v -> handler.handle(Future.failedFuture("renew cluster slots fail, redis cluster is down.")));
        }
    }

    int getNodeNumber() {
        return nodesCache.size();
    }

    List<RedisConnection> getShuffledNodesPool() {
        List<RedisConnection> pools = new ArrayList<>();
        pools.addAll(this.nodesCache.values());
        Collections.shuffle(pools);
        return pools;
    }

    private HostAndPort generateHostAndPort(JsonArray hostInfo) {
        return new HostAndPort(hostInfo.getString(0), hostInfo.getInteger(1));
    }

    private void assignSlotsToNode(JsonArray slotInfo, RedisOptions options) {
        RedisConnection redis = nodesCache.get(getNodeKey(options));

        if(redis == null) {
            setNodeIfNotExist(options);
            redis = nodesCache.get(getNodeKey(options));
        }

        int begin = slotInfo.getInteger(0);
        int end = slotInfo.getInteger(1);

        for(int i = begin; i <= end; i++) {
            slotsCache.put(i, redis);
        }
    }

    private void setNodeIfNotExist(RedisOptions options) {
        String nodeKey = getNodeKey(options);
        if(!nodesCache.containsKey(nodeKey)) {
            RedisConnection redis = new RedisConnection(vertx, options, null);
            nodesCache.put(nodeKey, redis);
        }
    }

    private String getNodeKey(HostAndPort hostAndPort) {
        return hostAndPort.getHost() + ":" + hostAndPort.getPort();
    }

    private String getNodeKey(RedisOptions options) {
        return options.getHost() + ":" + options.getPort();
    }

    private Context getContext(Vertx vertx) {
        Context ctx = Vertx.currentContext();
        if (ctx == null) {
            ctx = vertx.getOrCreateContext();
        } else if (!ctx.isEventLoopContext()) {
            VertxInternal vi = (VertxInternal) vertx;
            ctx = vi.createEventLoopContext(null, null, new JsonObject(), Thread.currentThread().getContextClassLoader());
        }
        return ctx;
    }

    private void runOnContext(Handler<Void> handler) {
        if (Vertx.currentContext() == context && Context.isOnEventLoopThread()) {
            handler.handle(null);
        } else {
            context.runOnContext(handler);
        }
    }
}
