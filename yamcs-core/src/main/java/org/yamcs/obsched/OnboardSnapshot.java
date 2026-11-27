package org.yamcs.obsched;

import java.util.List;

/**
 * The content of the on-board schedule, as read back from the spacecraft.
 *
 * @param time
 *            when the snapshot was taken (generation time of the report)
 * @param items
 *            the items in the on-board schedule
 */
public record OnboardSnapshot(long time, List<OnboardItem> items) {

    /**
     * One item of the on-board schedule.
     *
     * @param ref
     *            adapter-specific reference, the same as the one assigned to the entry at upload
     * @param releaseTime
     *            release time as known on board
     * @param description
     *            human readable description (shown for foreign items)
     */
    public record OnboardItem(String ref, long releaseTime, String description) {
    }
}
