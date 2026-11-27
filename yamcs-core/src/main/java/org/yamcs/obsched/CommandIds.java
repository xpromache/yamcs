package org.yamcs.obsched;

import org.yamcs.protobuf.Commanding.CommandId;

/**
 * Conversion between {@link CommandId} and the string identifier used by the API.
 */
final class CommandIds {

    private CommandIds() {
    }

    static String toStringIdentifier(CommandId commandId) {
        String id = commandId.getGenerationTime() + "-";
        if (commandId.hasOrigin() && !"".equals(commandId.getOrigin())) {
            id += commandId.getOrigin() + "-";
        }
        return id + commandId.getSequenceNumber();
    }

    static CommandId fromStringIdentifier(String commandName, String id) {
        var b = CommandId.newBuilder().setCommandName(commandName);
        int firstDash = id.indexOf('-');
        b.setGenerationTime(Long.parseLong(id.substring(0, firstDash)));
        int lastDash = id.lastIndexOf('-');
        b.setSequenceNumber(Integer.parseInt(id.substring(lastDash + 1)));
        b.setOrigin(firstDash != lastDash ? id.substring(firstDash + 1, lastDash) : "");
        return b.build();
    }
}
