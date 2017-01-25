package io.vertx.redis;

/**
 * Created by Caijt on 2017/1/23.
 */
public class HostAndPort {
    private String host;
    private Integer port;

    public HostAndPort() { }

    public HostAndPort(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "{host='" + host + "', port=" + port + '}';
    }
}
