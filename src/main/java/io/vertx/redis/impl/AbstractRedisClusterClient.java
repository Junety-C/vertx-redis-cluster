package io.vertx.redis.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisCluster;
import io.vertx.redis.RedisClusterOptions;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by caijt on 2017/1/24.
 */
public abstract class AbstractRedisClusterClient implements RedisCluster {

    private final RedisClusterConnection redisCluster;

    private final String encoding;
    private final Charset charset;
    private final Charset binaryCharset;
    private final String baseAddress;

    AbstractRedisClusterClient(Vertx vertx, RedisClusterOptions config) {
        this.encoding = config.getRedisOptions().getEncoding();
        this.charset = Charset.forName(encoding);
        this.binaryCharset = Charset.forName("iso-8859-1");
        this.baseAddress = config.getRedisOptions().getAddress();

        redisCluster = new RedisClusterConnection(vertx, config);
    }

    @Override
    public synchronized void close(Handler<AsyncResult<Void>> handler) {
        redisCluster.disconnect(handler);
    }

    final void sendString(final RedisCommand command, final List<?> args, int slot, final Handler<AsyncResult<String>> resultHandler) {
        send(command, args, slot, String.class, false, resultHandler);
    }

    final void sendLong(final RedisCommand command, final List<?> args, int slot, final Handler<AsyncResult<Long>> resultHandler) {
        send(command, args, slot, Long.class, false, resultHandler);
    }

    final void sendVoid(final RedisCommand command, final List<?> args, int slot, final Handler<AsyncResult<Void>> resultHandler) {
        send(command, args, slot, Void.class, false, resultHandler);
    }

    final void sendJsonArray(final RedisCommand command, final List<?> args, int slot, final Handler<AsyncResult<JsonArray>> resultHandler) {
        send(command, args, slot, JsonArray.class, false, resultHandler);
    }

    final void sendJsonObject(final RedisCommand command, final List<?> args, int slot, final Handler<AsyncResult<JsonObject>> resultHandler) {
        send(command, args, slot, JsonObject.class, false, resultHandler);
    }

    final <T> void send(final RedisCommand command, final List<?> redisArgs, int slot, final Class<T> returnType,
                        final boolean binary, final Handler<AsyncResult<T>> resultHandler) {

        final Command<T> cmd = new Command<>(Vertx.currentContext(), command, redisArgs, binary ? binaryCharset : charset, getResponseTransformFor(command), returnType).handler(resultHandler);
        redisCluster.send(new ClusterCommand(slot, cmd));
    }

    private ResponseTransform getResponseTransformFor(RedisCommand command) {
        if (command == RedisCommand.HGETALL) {
            return ResponseTransform.HASH;
        }
        if (command == RedisCommand.INFO) {
            return ResponseTransform.INFO;
        }

        if (command == RedisCommand.EVAL || command == RedisCommand.EVALSHA) {
            return ResponseTransform.ARRAY;
        }

        return ResponseTransform.NONE;
    }
}
