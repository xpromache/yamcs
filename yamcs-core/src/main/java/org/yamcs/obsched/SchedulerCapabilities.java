package org.yamcs.obsched;

import java.util.Set;

import org.yamcs.protobuf.obsched.EntryState;
import org.yamcs.protobuf.obsched.SchedulerCapabilitiesInfo;

/**
 * What an on-board scheduler can do, as far as the ground model is concerned.
 *
 * @param readBack
 *            the on-board schedule can be read back
 * @param entryLevelEdit
 *            single entries can be inserted and deleted (otherwise the whole schedule has to be replaced)
 * @param observableStates
 *            the entry states that the adapter can establish from telemetry
 * @param minLeadTime
 *            minimum time in milliseconds between a sync and the release time of an entry for the entry to be
 *            uploaded
 */
public record SchedulerCapabilities(boolean readBack, boolean entryLevelEdit, Set<EntryState> observableStates,
        long minLeadTime) {

    public SchedulerCapabilitiesInfo toProto() {
        return SchedulerCapabilitiesInfo.newBuilder()
                .setReadBack(readBack)
                .setEntryLevelEdit(entryLevelEdit)
                .addAllObservableStates(observableStates)
                .setMinLeadTime(minLeadTime)
                .build();
    }
}
