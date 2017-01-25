package io.vertx.redis.exception;

/**
 * Created by Caijt on 2016/12/31.
 */
public class RedisException extends RuntimeException {

    public RedisException(String message) {
        super(message);
    }

    public RedisException(Throwable e) {
        super(e);
    }

    public RedisException(String message, Throwable cause) {
        super(message, cause);
    }
}