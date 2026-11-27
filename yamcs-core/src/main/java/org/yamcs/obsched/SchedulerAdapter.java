package org.yamcs.obsched;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.YConfiguration;

/**
 * Service provider interface implemented for each on-board scheduling framework (PUS ST[11], cFS SC, F´, C2A...).
 * <p>
 * The generic {@link OnboardSchedulerService} keeps the ground model, computes what has to change and records the
 * evidence; the adapter knows how to talk to the on-board scheduler: how to insert and delete entries, how to read the
 * schedule back and how to recognise execution evidence in the telemetry.
 * <p>
 * Adapters are created by reflection and must have a public no-argument constructor.
 */
public interface SchedulerAdapter {

    /**
     * @return the specification of the adapter configuration (the {@code adapterArgs} of the service)
     */
    default Spec getSpec() {
        return Spec.ANY;
    }

    /**
     * Called once, when the service is initialised.
     */
    void init(SchedulerContext ctx, YConfiguration config) throws InitException;

    /**
     * @return short identifier of the adapter type, for example {@code pus11}
     */
    String getType();

    SchedulerCapabilities getCapabilities();

    /**
     * @return the specification of the adapter-specific entry options, for example the PUS sub-schedule and group
     */
    default Spec getEntryOptionsSpec() {
        return new Spec();
    }

    /**
     * @return names of the entry options that partition the schedule (for example the PUS sub-schedule and group),
     *         offered as filters in the user interface. The default value of the option in the
     *         {@link #getEntryOptionsSpec() spec} is taken as the value of entries that do not set it.
     */
    default Set<String> getFilterableOptions() {
        return Set.of();
    }

    /**
     * Starts listening to telemetry for execution evidence.
     */
    void start();

    void stop();

    /**
     * Reads back the on-board schedule. Only called if {@link SchedulerCapabilities#readBack()} is true.
     * <p>
     * This usually sends a command and waits for the report, so it requires contact with the spacecraft.
     */
    CompletableFuture<OnboardSnapshot> readOnboard(SyncContext ctx);

    /**
     * Computes the steps that bring the on-board schedule in line with the ground entries.
     * <p>
     * The entries have already been reconciled with the snapshot (if any). The adapter has to:
     * <ul>
     * <li>delete on board the entries with intent DELETE_PENDING that are (or may be) on board;</li>
     * <li>replace on board the entries that are on board but have been modified since their upload;</li>
     * <li>insert the PLANNED entries that are NOT_ONBOARD and whose release time is at or after {@code cutover}.</li>
     * </ul>
     *
     * @param entries
     *            copies of the non-deleted entries of the scheduler, sorted by release time
     * @param snapshot
     *            the on-board schedule, or null if the adapter cannot read it back
     * @param cutover
     *            entries due before this time cannot be uploaded anymore
     */
    SyncPlan planSync(List<ScheduleEntry> entries, OnboardSnapshot snapshot, long cutover);
}
