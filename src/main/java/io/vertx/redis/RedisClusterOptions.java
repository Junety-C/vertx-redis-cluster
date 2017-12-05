package io.vertx.redis;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by caijt on 2017/1/24.
 */
public class RedisClusterOptions {

    private List<HostAndPort> nodes;
    private RedisOptions redisOptions;

    public RedisClusterOptions() {
        this.nodes = new ArrayList<>();
        this.redisOptions = new RedisOptions();
    }

    public RedisClusterOptions addNode(HostAndPort hostAndPort) {
        nodes.add(hostAndPort);
        return this;
    }

    public HostAndPort getNode(int index) {
        return nodes.get(index);
    }

    public RedisOptions getRedisOptions() {
        return redisOptions;
    }

    public RedisOptions cloneRedisOptions() {
        return new RedisOptions().setAddress(redisOptions.getAddress())
                .setAuth(redisOptions.getAuth())
                .setEncoding(redisOptions.getEncoding())
                .setSelect(redisOptions.getSelect())
                .setBinary(redisOptions.isBinary());
    }

    public void setRedisOptions(RedisOptions redisOptions) {
        this.redisOptions = redisOptions;
    }

    public List<HostAndPort> getNodes() {
        return nodes;
    }

    public void setNodes(List<HostAndPort> nodes) {
        this.nodes = nodes;
    }

    public int size() {
        return nodes.size();
    }

    public RedisOptions getRedisOptions(int index) {
        return new RedisOptions().setAddress(redisOptions.getAddress())
                .setAuth(redisOptions.getAuth())
                .setEncoding(redisOptions.getEncoding())
                .setSelect(redisOptions.getSelect())
                .setBinary(redisOptions.isBinary())
                .setHost(nodes.get(index).getHost())
                .setPort(nodes.get(index).getPort());
    }
}
