package io.vertx.redis.impl;

import io.vertx.core.*;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisClusterOptions;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Caijt on 2017/1/23.
 */
class RedisClusterConnection {

    private final Vertx vertx;
    private final Context context;
    private final RedisClusterOptions config;

    private final int MAX_PENDING_QUEUE_SIZE = 50000;
    private final Queue<ClusterCommand<?>> pending = new LinkedList<>();

    private RedisClusterCache cache;

    private static RedisCommand askingCommand;

    static {
        // RedisCommand.class not contains asking command, we need asking command to handle ASK error
        EnumUtils.addEnum(RedisCommand.class, "ASKING", "asking");
        askingCommand = RedisCommand.valueOf("ASKING");
    }

    /**
     * redis cluster connection status
     */
    public enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        RECONNECTING,
        DISCONNECTING,
        ERROR
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.DISCONNECTED);

    /**
     * Create a RedisClusterConnection
     */
    RedisClusterConnection(Vertx vertx, RedisClusterOptions config) {
        this.vertx = vertx;
        this.context = this.getContext(vertx);
        this.config = config;
        this.cache = new RedisClusterCache(vertx);
    }

    /**
     * connect and initialize slots cache
     */
    private void connect() {
        if (state.compareAndSet(State.DISCONNECTED, State.CONNECTING)) {
            this.initializeSlotsCache(vertx, config, 0, ar -> {
                if (ar.failed()) {
                    if (state.compareAndSet(State.CONNECTING, State.ERROR)) {
                        // clean up any pending command
                        clearQueue(pending, ar.cause());
                    }
                    state.set(State.DISCONNECTED);
                } else {
                    resendPending();
                }
            });
        }
    }

    /**
     * initialize slots cache by redis cluster options
     */
    private void initializeSlotsCache(Vertx vertx, RedisClusterOptions config, int index, Handler<AsyncResult<Void>> handler) {
        if (index < config.size()) {
            RedisClient client = RedisClient.create(vertx, config.getRedisOptions(index));
            Future<Void> future = Future.future();
            cache.discoverClusterNodesAndSlots(client, config, future.setHandler(ar -> {
                // if current redis options connect fail, try the next one
                if (ar.failed()) {
                    initializeSlotsCache(vertx, config, index + 1, handler);
                } else {
                    runOnContext(v -> handler.handle(Future.succeededFuture()));
                }
            }));
        } else {
            // when all redis options connect fail
            runOnContext(v -> handler.handle(Future.failedFuture("connect redis cluster fail.")));
        }
    }

    /**
     * close all redis connection
     */
    void disconnect(Handler<AsyncResult<Void>> closeHandler) {
        final Command<Void> cmd = new Command<>(context, RedisCommand.QUIT, null, Charset.defaultCharset(), ResponseTransform.NONE, Void.class);
        final AtomicInteger cnt = new AtomicInteger(0);
        switch (state.get()) {
            case CONNECTING:
                cmd.handler(v -> {
                    if (state.compareAndSet(State.CONNECTED, State.DISCONNECTING) || state.get() == State.DISCONNECTING) {
                        if (cnt.incrementAndGet() == cache.getNodeNumber()) {
                            clearQueue(pending, "Connection closed");
                            closeHandler.handle(Future.succeededFuture());
                        }
                    }
                });
                pending.add(new ClusterCommand<>(-1, cmd, closeHandler));
                break;
            case CONNECTED:
                cmd.handler(v -> {
                    if (state.compareAndSet(State.CONNECTED, State.DISCONNECTING) || state.get() == State.DISCONNECTING) {
                        if (cnt.incrementAndGet() == cache.getNodeNumber()) {
                            clearQueue(pending, "Connection closed");
                            closeHandler.handle(Future.succeededFuture());
                        }
                    }
                });
                sendAll(cmd);
                break;
            case DISCONNECTING:
            case ERROR:
                // eventually will become DISCONNECTED
            case DISCONNECTED:
                closeHandler.handle(Future.succeededFuture());
                break;
        }
    }

    /**
     * send a cluster command to a redis connection
     */
    <T> void send(final ClusterCommand<T> clusterCommand) {
        // start the handshake if not connected
        if (state.get() == State.DISCONNECTED) {
            connect();
        }

        commandHandlerWrap(clusterCommand);

        // write to the socket in the netSocket context
        runOnContext(v -> {
            switch (state.get()) {
                case CONNECTED:
                case RECONNECTING:
                    RedisConnection redisConnection = cache.getRedis(clusterCommand.getSlot());
                    if (redisConnection != null) {
                        redisConnection.send(clusterCommand.getCommand());
                    } else {
                        clusterCommand.getResultHandler().handle(Future.failedFuture("slot " + clusterCommand.getSlot() + " connection is null"));
                    }
                    break;
                case CONNECTING:
                case ERROR:
                case DISCONNECTING:
                case DISCONNECTED:
                    addCommandToPendingQueue(clusterCommand);
                    break;
            }
        });
    }

    private <T> void addCommandToPendingQueue(ClusterCommand<T> clusterCommand) {
        // avoid OOM, you can adjust MAX_PENDING_QUEUE_SIZE
        if (pending.size() >= MAX_PENDING_QUEUE_SIZE) {
            ClusterCommand<?> firstCommand = pending.poll();
            if (firstCommand != null) {
                firstCommand.getResultHandler().handle(Future.failedFuture("OUT OT PENDING QUEUE SIZE"));
            }
        }
        pending.add(clusterCommand);
    }


    private <T> void commandHandlerWrap(final ClusterCommand<T> clusterCommand) {
        clusterCommand.getCommand().handler(ar -> {
            // when error occurs, such as slot move, connection refused. it will renew the slot cache
            if (ar.cause() != null && clusterCommand.isResend()) {
                String message = ar.cause().getMessage();
                if (message.contains("MOVED")) {
                    // MOVED -> renew cache and redirect
                    clusterCommand.setResend(false);
                    renewSlotCache();
                    String nodeKey = message.substring(message.lastIndexOf(" ") + 1);
                    RedisConnection redisConnection = cache.getOrCreateRedis(nodeKey, config);
                    redisConnection.send(clusterCommand.getCommand());
                } else if (message.contains("ASK")) {
                    // ASK -> redirect
                    clusterCommand.setResend(false);
                    String nodeKey = message.substring(message.lastIndexOf(" ") + 1);
                    RedisConnection redisConnection = cache.getOrCreateRedis(nodeKey, config);
                    redisConnection.send(askingCommand());
                    redisConnection.send(clusterCommand.getCommand());
                } else {
                    // other ERROR
                    renewSlotCache();
                    clusterCommand.getResultHandler().handle(ar);
                }
            } else {
                clusterCommand.getResultHandler().handle(ar);
            }
        });
    }

    private Command<String> askingCommand() {
        return new Command<>(
                Vertx.currentContext(),
                askingCommand,
                Collections.emptyList(),
                Charset.forName("UTF-8"),
                ResponseTransform.NONE,
                String.class).handler(ar -> {});
    }

    /**
     * send to all redis connection
     */
    void sendAll(final Command<?> command) {
        List<RedisConnection> nodesPool = cache.getShuffledNodesPool();
        for (RedisConnection connection : nodesPool) {
            connection.send(command);
        }
    }

    /**
     * when one of the redis connection down or data move, renew the redis cluster slots cache
     */
    private void renewSlotCache() {
        if (state.compareAndSet(State.CONNECTED, State.RECONNECTING)) {
            cache.renewClusterSlots(config, ar -> {
                if (ar.failed()) {
                    if (state.compareAndSet(State.RECONNECTING, State.ERROR)) {
                        // clean up any pending command
                        clearQueue(pending, ar.cause());
                    }
                    state.set(State.DISCONNECTED);
                } else {
                    state.compareAndSet(State.RECONNECTING, State.CONNECTED);
                }
            });
        }
    }

    private void resendPending() {
        if (state.compareAndSet(State.CONNECTING, State.CONNECTED)) {
            // we are connected so clean up the pending queue
            ClusterCommand<?> clusterCommand;
            while ((clusterCommand = pending.poll()) != null) {
                if (clusterCommand.getSlot() == -1) {
                    sendAll(clusterCommand.getCommand());
                } else {
                    send(clusterCommand);
                }
            }
        }
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

    private static void clearQueue(Queue<ClusterCommand<?>> q, String message) {
        ClusterCommand<?> cmd;

        while ((cmd = q.poll()) != null) {
            cmd.getCommand().handle(Future.failedFuture(message));
        }
    }

    private static void clearQueue(Queue<ClusterCommand<?>> q, Throwable cause) {
        ClusterCommand<?> cmd;

        while ((cmd = q.poll()) != null) {
            cmd.getCommand().handle(Future.failedFuture(cause));
        }
    }
}
