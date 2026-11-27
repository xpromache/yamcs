package org.yamcs.obsched;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.yamcs.cmdhistory.Attribute;
import org.yamcs.cmdhistory.CommandHistoryConsumer;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.parameter.Value;
import org.yamcs.protobuf.Commanding.CommandId;

/**
 * A command sent by the scheduler engine on behalf of an adapter. Gives access to the command history attributes as
 * they are published (for example the CCSDS sequence count filled in by the post-processor).
 */
public class IssuedCommand implements CommandHistoryConsumer {

    private final CommandId commandId;
    private final Map<String, Value> attributes = new HashMap<>();
    private final Map<String, CompletableFuture<Value>> waiting = new HashMap<>();
    private String failure;

    IssuedCommand(CommandId commandId) {
        this.commandId = commandId;
    }

    public CommandId getCommandId() {
        return commandId;
    }

    /**
     * @return the command identifier as used by the API
     */
    public String getId() {
        return CommandIds.toStringIdentifier(commandId);
    }

    /**
     * Returns a future that completes with the value of the given command history attribute as soon as it is
     * published. The future completes exceptionally if the command fails on the ground before.
     */
    public synchronized CompletableFuture<Value> attribute(String key) {
        var v = attributes.get(key);
        if (v != null) {
            return CompletableFuture.completedFuture(v);
        }
        if (failure != null) {
            return CompletableFuture.failedFuture(new CommandFailedException(failure));
        }
        return waiting.computeIfAbsent(key, k -> new CompletableFuture<>());
    }

    synchronized void update(String key, Value value) {
        attributes.put(key, value);
        var f = waiting.remove(key);
        if (f != null) {
            f.complete(value);
        }
        if (key.equals(CommandHistoryPublisher.CommandComplete_KEY + CommandHistoryPublisher.SUFFIX_STATUS)
                && "NOK".equals(value.getStringValue())) {
            fail();
        }
    }

    private void fail() {
        var msg = attributes.get(CommandHistoryPublisher.CommandComplete_KEY + CommandHistoryPublisher.SUFFIX_MESSAGE);
        failure = "Command " + commandId.getCommandName() + " failed"
                + (msg == null ? "" : ": " + msg.getStringValue());
        for (var f : waiting.values()) {
            f.completeExceptionally(new CommandFailedException(failure));
        }
        waiting.clear();
    }

    @Override
    public void addedCommand(PreparedCommand pc) {
        // not used: the subscription is per command
    }

    @Override
    public void updatedCommand(CommandId cmdId, long time, List<Attribute> attrs) {
        for (var a : attrs) {
            update(a.getKey(), a.getValue());
        }
    }

    @SuppressWarnings("serial")
    public static class CommandFailedException extends RuntimeException {
        public CommandFailedException(String msg) {
            super(msg);
        }
    }
}
