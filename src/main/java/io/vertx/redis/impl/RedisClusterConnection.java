package io.vertx.redis.impl;

import io.vertx.core.*;
import io.vertx.redis.RedisClusterOptions;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Caijt on 2017/1/23.
 */
class RedisClusterConnection {

    private final Context context;
    private final RedisClusterOptions config;

    private final Queue<ClusterCommand> pending = new LinkedList<>();

    private final RedisClusterClient redisClusterClient;

    private enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        ERROR
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.DISCONNECTED);


    RedisClusterConnection(Vertx vertx, RedisClusterOptions config) {
        this.context = vertx.getOrCreateContext();
        this.config = config;
        redisClusterClient = new RedisClusterClient(vertx);
    }

    private void connect() {
        if (state.compareAndSet(State.DISCONNECTED, State.CONNECTING)) {
            redisClusterClient.connect(config, ar -> {
                if(ar.failed()) {
                    runOnContext(v -> {
                        if(state.compareAndSet(State.CONNECTING, State.ERROR)) {
                            // clean up any pending command
                            clearQueue(pending, ar.cause());
                        }
                        state.set(State.DISCONNECTED);
                    });
                } else {
                    runOnContext(v -> {
                        resendPending();
                    });
                }
            });
        }
    }

    int getConnectionNumber() {
        return redisClusterClient.getConnectionNumber();
    }

    void disconnect(Handler<AsyncResult<Void>> closeHandler) {
        switch (state.get()) {
            case CONNECTING:
                // eventually will become connected
            case CONNECTED:
                // TODO disconnect

                break;
            case ERROR:
                // eventually will become DISCONNECTED
            case DISCONNECTED:
                closeHandler.handle(Future.succeededFuture());
                break;
        }
    }

    void send(final ClusterCommand clusterCommand) {
        // start the handshake if not connected
        if (state.get() == State.DISCONNECTED) {
            connect();
        }

        // write to the socket in the netSocket context
        runOnContext(v -> {
            switch (state.get()) {
                case CONNECTED:
                    redisClusterClient.send(clusterCommand);
                    break;
                case CONNECTING:
                case ERROR:
                case DISCONNECTED:
                    pending.add(clusterCommand);
                    break;
            }
        });
    }

    private void resendPending() {
        runOnContext(v -> {
            ClusterCommand clusterCommand;
            if (state.compareAndSet(State.CONNECTING, State.CONNECTED)) {
                // we are connected so clean up the pending queue
                while ((clusterCommand = pending.poll()) != null) {
                    redisClusterClient.send(clusterCommand);
                }
            }
        });
    }

    private void runOnContext(Handler<Void> handler) {
        if (Vertx.currentContext() == context) {
            handler.handle(null);
        } else {
            context.runOnContext(handler);
        }
    }

    private static void clearQueue(Queue<ClusterCommand> q, String message) {
        ClusterCommand cmd;

        while ((cmd = q.poll()) != null) {
            cmd.getCommand().handle(Future.failedFuture(message));
        }
    }

    private static void clearQueue(Queue<ClusterCommand> q, Throwable cause) {
        ClusterCommand cmd;

        while ((cmd = q.poll()) != null) {
            cmd.getCommand().handle(Future.failedFuture(cause));
        }
    }
}
