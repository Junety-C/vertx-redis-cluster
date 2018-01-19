package io.vertx.redis.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisCluster;
import io.vertx.redis.RedisClusterOptions;
import io.vertx.redis.op.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.vertx.redis.impl.RedisCommand.*;

/**
 * Created by Caijt on 2017/1/23.
 */
public class RedisClusterImpl extends AbstractRedisClusterClient {

    public RedisClusterImpl(Vertx vertx, RedisClusterOptions config) {
        super(vertx, config);
    }

    @Override
    public RedisCluster append(String key, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(APPEND, toPayload(key, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster blpop(String key, int seconds, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(BLPOP, toPayload(key, seconds), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster brpop(String key, int seconds, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(BRPOP, toPayload(key, seconds), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster brpoplpush(String key, String destkey, int seconds, Handler<AsyncResult<String>> handler) {
        sendString(BRPOPLPUSH, toPayload(key, destkey, seconds), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster decr(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(DECR, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster decrby(String key, long decrement, Handler<AsyncResult<Long>> handler) {
        sendLong(DECRBY, toPayload(key, decrement), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster del(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(DEL, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster exists(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(EXISTS, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster expire(String key, int seconds, Handler<AsyncResult<Long>> handler) {
        sendLong(EXPIRE, toPayload(key, seconds), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster expireat(String key, long seconds, Handler<AsyncResult<Long>> handler) {
        sendLong(EXPIREAT, toPayload(key, seconds), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster get(String key, Handler<AsyncResult<String>> handler) {
        sendString(GET, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster getBinary(String key, Handler<AsyncResult<Buffer>> handler) {
        send(GET, toPayload(key), RedisClusterCRC16.getSlot(key), Buffer.class, true, handler);
        return this;
    }

    @Override
    public RedisCluster getbit(String key, long offset, Handler<AsyncResult<Long>> handler) {
        sendLong(GETBIT, toPayload(key, offset), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster getrange(String key, long start, long end, Handler<AsyncResult<String>> handler) {
        sendString(GETRANGE, toPayload(key, start, end), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster getset(String key, String value, Handler<AsyncResult<String>> handler) {
        sendString(GETSET, toPayload(key, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hdel(String key, String field, Handler<AsyncResult<Long>> handler) {
        sendLong(HDEL, toPayload(key, field), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hexists(String key, String field, Handler<AsyncResult<Long>> handler) {
        sendLong(HEXISTS, toPayload(key, field), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hget(String key, String field, Handler<AsyncResult<String>> handler) {
        sendString(HGET, toPayload(key, field), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hgetall(String key, Handler<AsyncResult<JsonObject>> handler) {
        sendJsonObject(HGETALL, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hincrby(String key, String field, long increment, Handler<AsyncResult<Long>> handler) {
        sendLong(HINCRBY, toPayload(key, field, increment), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hincrbyfloat(String key, String field, double increment, Handler<AsyncResult<String>> handler) {
        sendString(HINCRBYFLOAT, toPayload(key, field, increment), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hkeys(String key, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(HKEYS, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hlen(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(HLEN, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hmget(String key, List<String> fields, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(HMGET, toPayload(key, fields), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hmset(String key, JsonObject values, Handler<AsyncResult<String>> handler) {
        sendString(HMSET, toPayload(key, values), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hset(String key, String field, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(HSET, toPayload(key, field, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hsetnx(String key, String field, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(HSETNX, toPayload(key, field, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hvals(String key, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(HVALS, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster incr(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(INCR, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster incrby(String key, long increment, Handler<AsyncResult<Long>> handler) {
        sendLong(INCRBY, toPayload(key, increment), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster incrbyfloat(String key, double increment, Handler<AsyncResult<String>> handler) {
        sendString(INCRBYFLOAT, toPayload(key, increment), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster lindex(String key, int index, Handler<AsyncResult<String>> handler) {
        sendString(LINDEX, toPayload(key, index), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster linsert(String key, InsertOptions option, String pivot, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(LINSERT, toPayload(key, option.name(), pivot, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster llen(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(LLEN, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster lpop(String key, Handler<AsyncResult<String>> handler) {
        sendString(LPOP, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster lpushMany(String key, List<String> values, Handler<AsyncResult<Long>> handler) {
        sendLong(LPUSH, toPayload(key, values), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster lpush(String key, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(LPUSH, toPayload(key, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster lpushx(String key, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(LPUSHX, toPayload(key, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster lrange(String key, long from, long to, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(LRANGE, toPayload(key, from, to), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster lrem(String key, long count, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(LREM, toPayload(key, count, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster lset(String key, long index, String value, Handler<AsyncResult<String>> handler) {
        sendString(LSET, toPayload(key, index, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster ltrim(String key, long from, long to, Handler<AsyncResult<String>> handler) {
        sendString(LTRIM, toPayload(key, from, to), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster mget(String key, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(MGET, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster rpop(String key, Handler<AsyncResult<String>> handler) {
        sendString(RPOP, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster rpoplpush(String key, String destkey, Handler<AsyncResult<String>> handler) {
        sendString(RPOPLPUSH, toPayload(key, destkey), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster rpushMany(String key, List<String> values, Handler<AsyncResult<Long>> handler) {
        sendLong(RPUSH, toPayload(key, values), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster rpush(String key, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(RPUSH, toPayload(key, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster rpushx(String key, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(RPUSHX, toPayload(key, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster sadd(String key, String member, Handler<AsyncResult<Long>> handler) {
        sendLong(SADD, toPayload(key, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster saddMany(String key, List<String> members, Handler<AsyncResult<Long>> handler) {
        sendLong(SADD, toPayload(key, members), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster scard(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(SCARD, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster set(String key, String value, Handler<AsyncResult<Void>> handler) {
        sendVoid(SET, toPayload(key, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster setWithOptions(String key, String value, SetOptions options, Handler<AsyncResult<String>> handler) {
        sendString(SET, toPayload(key, value, options != null ? options.toJsonArray() : null), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster setex(String key, long seconds, String value, Handler<AsyncResult<String>> handler) {
        sendString(SETEX, toPayload(key, seconds, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster setnx(String key, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(SETNX, toPayload(key, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster setrange(String key, int offset, String value, Handler<AsyncResult<Long>> handler) {
        sendLong(SETRANGE, toPayload(key, offset, value), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster sismember(String key, String member, Handler<AsyncResult<Long>> handler) {
        sendLong(SISMEMBER, toPayload(key, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster smembers(String key, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(SMEMBERS, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster spop(String key, Handler<AsyncResult<String>> handler) {
        sendString(SPOP, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster srem(String key, String member, Handler<AsyncResult<Long>> handler) {
        sendLong(SREM, toPayload(key, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster sremMany(String key, List<String> members, Handler<AsyncResult<Long>> handler) {
        sendLong(SREM, toPayload(key, members), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster strlen(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(STRLEN, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster ttl(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(TTL, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster type(String key, Handler<AsyncResult<String>> handler) {
        sendString(TYPE, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zadd(String key, double score, String member, Handler<AsyncResult<Long>> handler) {
        sendLong(ZADD, toPayload(key, score, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zaddMany(String key, Map<String, Double> members, Handler<AsyncResult<Long>> handler) {
        // flip from <String, Double> to <Double, String> when wrapping
        Stream flipped = members.entrySet().stream().map(e -> new Object[]{e.getValue(), e.getKey()});
        sendLong(ZADD, toPayload(key, flipped), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zcard(String key, Handler<AsyncResult<Long>> handler) {
        sendLong(ZCARD, toPayload(key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zcount(String key, double min, double max, Handler<AsyncResult<Long>> handler) {
        String minVal = (min == Double.NEGATIVE_INFINITY) ? "-inf" : String.valueOf(min);
        String maxVal = (max == Double.POSITIVE_INFINITY) ? "+inf" : String.valueOf(max);
        sendLong(ZCOUNT, toPayload(key, minVal, maxVal), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zincrby(String key, double increment, String member, Handler<AsyncResult<String>> handler) {
        sendString(ZINCRBY, toPayload(key, increment, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zlexcount(String key, String min, String max, Handler<AsyncResult<Long>> handler) {
        sendLong(ZLEXCOUNT, toPayload(key, min, max), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrange(String key, long start, long stop, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZRANGE, toPayload(key, start, stop), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrangeWithOptions(String key, long start, long stop, RangeOptions options, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZRANGE, toPayload(key, start, stop, options != null ? options.toJsonArray() : null), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrangebylex(String key, String min, String max, LimitOptions options, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZRANGEBYLEX, toPayload(key, min, max, options != null ? options.toJsonArray() : null), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrangebyscore(String key, String min, String max, RangeLimitOptions options, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZRANGEBYSCORE, toPayload(key, min, max, options != null ? options.toJsonArray() : null), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrank(String key, String member, Handler<AsyncResult<Long>> handler) {
        sendLong(ZRANK, toPayload(key, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrem(String key, String member, Handler<AsyncResult<Long>> handler) {
        sendLong(ZREM, toPayload(key, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zremMany(String key, List<String> members, Handler<AsyncResult<Long>> handler) {
        sendLong(ZREM, toPayload(key, members), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zremrangebylex(String key, String min, String max, Handler<AsyncResult<Long>> handler) {
        sendLong(ZREMRANGEBYLEX, toPayload(key, min, max), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zremrangebyrank(String key, long start, long stop, Handler<AsyncResult<Long>> handler) {
        sendLong(ZREMRANGEBYRANK, toPayload(key, start, stop), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zremrangebyscore(String key, String min, String max, Handler<AsyncResult<Long>> handler) {
        sendLong(ZREMRANGEBYSCORE, toPayload(key, min, max), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrevrange(String key, long start, long stop, RangeOptions options, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZREVRANGE, toPayload(key, start, stop, options != null ? options.toJsonArray() : null), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrevrangebylex(String key, String max, String min, LimitOptions options, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZREVRANGEBYLEX, toPayload(key, max, min, options != null ? options.toJsonArray() : null), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrevrangebyscore(String key, String max, String min, RangeLimitOptions options, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZREVRANGEBYSCORE, toPayload(key, max, min, options != null ? options.toJsonArray() : null), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrevrank(String key, String member, Handler<AsyncResult<Long>> handler) {
        sendLong(ZREVRANK, toPayload(key, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zscore(String key, String member, Handler<AsyncResult<String>> handler) {
        sendString(ZSCORE, toPayload(key, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hget(byte[] key, byte[] field, Handler<AsyncResult<String>> handler) {
        sendString(HGET, toPayload(key, field), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hmget(byte[] key, List<byte[]> fields, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(HMGET, toPayload(key, fields), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster smembers(byte[] key, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(SMEMBERS, toPayload((Object) key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster sismember(byte[] key, byte[] member, Handler<AsyncResult<Long>> handler) {
        sendLong(SISMEMBER, toPayload(key, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster hincrby(byte[] key, byte[] field, long increment, Handler<AsyncResult<String>> handler) {
        sendString(HINCRBY, toPayload(key, field, increment), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrange(byte[] key, long start, long stop, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZRANGE, toPayload(key, start, stop), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zadd(byte[] key, double score, byte[] member, Handler<AsyncResult<Long>> handler) {
        sendLong(ZADD, toPayload(key, score, member), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zremrangebyscore(byte[] key, double start, double end, Handler<AsyncResult<Long>> handler) {
        sendLong(ZREMRANGEBYSCORE, toPayload(key, start, end), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster scard(byte[] key, Handler<AsyncResult<Long>> handler) {
        sendLong(SCARD, toPayload((Object) key), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster zrangebyscore(byte[] key, double min, double max, RangeLimitOptions options, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(ZRANGEBYSCORE, toPayload(key, min, max, options != null ? options.toJsonArray() : null), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    @Override
    public RedisCluster expire(byte[] key, int seconds, Handler<AsyncResult<String>> handler) {
        sendString(EXPIRE, toPayload(key, seconds), RedisClusterCRC16.getSlot(key), handler);
        return this;
    }

    private static List<?> toPayload(Object... parameters) {
        List<Object> result = new ArrayList<>(parameters.length);

        for (Object param : parameters) {
            // unwrap
            if (param instanceof JsonArray) {
                param = ((JsonArray) param).getList();
            }
            // unwrap
            if (param instanceof JsonObject) {
                param = ((JsonObject) param).getMap();
            }

            if (param instanceof Collection) {
                ((Collection) param).stream().filter(el -> el != null).forEach(result::add);
            } else if (param instanceof Map) {
                for (Map.Entry<?, ?> pair : ((Map<?, ?>) param).entrySet()) {
                    result.add(pair.getKey());
                    result.add(pair.getValue());
                }
            } else if (param instanceof Stream) {
                ((Stream) param).forEach(e -> {
                    if (e instanceof Object[]) {
                        for (Object item : (Object[]) e) {
                            result.add(item);
                        }
                    } else {
                        result.add(e);
                    }
                });
            } else if (param instanceof Buffer) {
                result.add(((Buffer) param).getBytes());
            } else if (param != null) {
                result.add(param);
            }
        }
        return result;
    }
}
