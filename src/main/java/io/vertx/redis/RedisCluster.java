package io.vertx.redis;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.impl.RedisClusterImpl;
import io.vertx.redis.op.*;

import java.util.List;
import java.util.Map;

/**
 * Created by Caijt on 2017/1/24.
 */
public interface RedisCluster {

    static RedisCluster create(Vertx vertx, RedisClusterOptions config) {
        return new RedisClusterImpl(vertx, config);
    }

    void close(Handler<AsyncResult<Void>> handler);

    RedisCluster append(String key, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster blpop(String key, int seconds, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster brpop(String key, int seconds, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster brpoplpush(String key, String destkey, int seconds, Handler<AsyncResult<String>> handler);

    RedisCluster decr(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster decrby(String key, long decrement, Handler<AsyncResult<Long>> handler);

    RedisCluster del(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster exists(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster expire(String key, int seconds, Handler<AsyncResult<Long>> handler);

    RedisCluster expireat(String key, long seconds, Handler<AsyncResult<Long>> handler);

    RedisCluster get(String key, Handler<AsyncResult<String>> handler);

    RedisCluster getBinary(String key, Handler<AsyncResult<Buffer>> handler);

    RedisCluster getbit(String key, long offset, Handler<AsyncResult<Long>> handler);

    RedisCluster getrange(String key, long start, long end, Handler<AsyncResult<String>> handler);

    RedisCluster getset(String key, String value, Handler<AsyncResult<String>> handler);

    RedisCluster hdel(String key, String field, Handler<AsyncResult<Long>> handler);

    RedisCluster hexists(String key, String field, Handler<AsyncResult<Long>> handler);

    RedisCluster hget(String key, String field, Handler<AsyncResult<String>> handler);

    RedisCluster hgetall(String key, Handler<AsyncResult<JsonObject>> handler);

    RedisCluster hincrby(String key, String field, long increment, Handler<AsyncResult<Long>> handler);

    RedisCluster hincrbyfloat(String key, String field, double increment, Handler<AsyncResult<String>> handler);

    RedisCluster hkeys(String key, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster hlen(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster hmget(String key, List<String> fields, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster hmset(String key, JsonObject values, Handler<AsyncResult<String>> handler);

    RedisCluster hset(String key, String field, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster hsetnx(String key, String field, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster hvals(String key, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster incr(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster incrby(String key, long increment, Handler<AsyncResult<Long>> handler);

    RedisCluster incrbyfloat(String key, double increment, Handler<AsyncResult<String>> handler);

    RedisCluster lindex(String key, int index, Handler<AsyncResult<String>> handler);

    RedisCluster linsert(String key, InsertOptions option, String pivot, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster llen(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster lpop(String key, Handler<AsyncResult<String>> handler);

    RedisCluster lpushMany(String key, List<String> values, Handler<AsyncResult<Long>> handler);

    RedisCluster lpush(String key, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster lpushx(String key, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster lrange(String key, long from, long to, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster lrem(String key, long count, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster lset(String key, long index, String value, Handler<AsyncResult<String>> handler);

    RedisCluster ltrim(String key, long from, long to, Handler<AsyncResult<String>> handler);

    RedisCluster mget(String key, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster rpop(String key, Handler<AsyncResult<String>> handler);

    RedisCluster rpoplpush(String key, String destkey, Handler<AsyncResult<String>> handler);

    RedisCluster rpushMany(String key, List<String> values, Handler<AsyncResult<Long>> handler);

    RedisCluster rpush(String key, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster rpushx(String key, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster sadd(String key, String member, Handler<AsyncResult<Long>> handler);

    RedisCluster saddMany(String key, List<String> members, Handler<AsyncResult<Long>> handler);

    RedisCluster scard(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster set(String key, String value, Handler<AsyncResult<Void>> handler);

    RedisCluster setWithOptions(String key, String value, SetOptions options, Handler<AsyncResult<String>> handler);

    RedisCluster setex(String key, long seconds, String value, Handler<AsyncResult<String>> handler);

    RedisCluster setnx(String key, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster setrange(String key, int offset, String value, Handler<AsyncResult<Long>> handler);

    RedisCluster sismember(String key, String member, Handler<AsyncResult<Long>> handler);

    RedisCluster smembers(String key, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster spop(String key, Handler<AsyncResult<String>> handler);

    RedisCluster srem(String key, String member, Handler<AsyncResult<Long>> handler);

    RedisCluster sremMany(String key, List<String> members, Handler<AsyncResult<Long>> handler);

    RedisCluster strlen(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster ttl(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster type(String key, Handler<AsyncResult<String>> handler);

    RedisCluster zadd(String key, double score, String member, Handler<AsyncResult<Long>> handler);

    RedisCluster zaddMany(String key, Map<String, Double> members, Handler<AsyncResult<Long>> handler);

    RedisCluster zcard(String key, Handler<AsyncResult<Long>> handler);

    RedisCluster zcount(String key, double min, double max, Handler<AsyncResult<Long>> handler);

    RedisCluster zincrby(String key, double increment, String member, Handler<AsyncResult<String>> handler);

    RedisCluster zlexcount(String key, String min, String max, Handler<AsyncResult<Long>> handler);

    RedisCluster zrange(String key, long start, long stop, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster zrangeWithOptions(String key, long start, long stop, RangeOptions options, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster zrangebylex(String key, String min, String max, LimitOptions options, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster zrangebyscore(String key, String min, String max, RangeLimitOptions options, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster zrank(String key, String member, Handler<AsyncResult<Long>> handler);

    RedisCluster zrem(String key, String member, Handler<AsyncResult<Long>> handler);

    RedisCluster zremMany(String key, List<String> members, Handler<AsyncResult<Long>> handler);

    RedisCluster zremrangebylex(String key, String min, String max, Handler<AsyncResult<Long>> handler);

    RedisCluster zremrangebyrank(String key, long start, long stop, Handler<AsyncResult<Long>> handler);

    RedisCluster zremrangebyscore(String key, String min, String max, Handler<AsyncResult<Long>> handler);

    RedisCluster zrevrange(String key, long start, long stop, RangeOptions options, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster zrevrangebylex(String key, String max, String min, LimitOptions options, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster zrevrangebyscore(String key, String max, String min, RangeLimitOptions options, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster zrevrank(String key, String member, Handler<AsyncResult<Long>> handler);

    RedisCluster zscore(String key, String member, Handler<AsyncResult<String>> handler);

    RedisCluster hget(byte[] key, byte[] field, Handler<AsyncResult<String>> handler);

    RedisCluster hmget(byte[] key, List<byte[]> fields, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster smembers(byte[] key, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster sismember(byte[] key, byte[] member, Handler<AsyncResult<Long>> handler);

    RedisCluster hincrby(byte[] key, byte[] field, long increment, Handler<AsyncResult<String>> handler);

    RedisCluster zrange(byte[] key, long start, long stop, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster zadd(byte[] key, double score, byte[] member, Handler<AsyncResult<Long>> handler);

    RedisCluster zremrangebyscore(final byte[] key, final double start, final double end, Handler<AsyncResult<Long>> handler);

    RedisCluster scard(final byte[] key, Handler<AsyncResult<Long>> handler);

    RedisCluster zrangebyscore(final byte[] key, double min, double max, RangeLimitOptions options, Handler<AsyncResult<JsonArray>> handler);

    RedisCluster expire(byte[] key, int seconds, Handler<AsyncResult<String>> handler);
}
