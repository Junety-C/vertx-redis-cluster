package io.vertx.redis.impl;

/**
 * Created by caijt on 2017/1/24.
 */
public class ClusterCommand {

    private int slot;
    private Command<?> command;

    public ClusterCommand() {}

    public ClusterCommand(int slot, Command<?> command) {
        this.slot = slot;
        this.command = command;
    }

    public int getSlot() {
        return slot;
    }

    public void setSlot(int slot) {
        this.slot = slot;
    }

    public Command<?> getCommand() {
        return command;
    }

    public void setCommand(Command<?> command) {
        this.command = command;
    }
}
