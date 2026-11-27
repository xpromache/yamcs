package org.yamcs.obsched;

import java.util.concurrent.ScheduledExecutorService;

import org.yamcs.Processor;
import org.yamcs.protobuf.obsched.EntryState;
import org.yamcs.protobuf.obsched.EvidenceSource;

/**
 * What the {@link OnboardSchedulerService} offers to its {@link SchedulerAdapter} for the lifetime of the service.
 */
public interface SchedulerContext {

    String getYamcsInstance();

    /**
     * @return the name of the scheduler (the service name)
     */
    String getSchedulerName();

    /**
     * @return the processor used for commanding and for the time
     */
    Processor getProcessor();

    /**
     * @return an executor that the adapter can use for timeouts and other delayed actions
     */
    ScheduledExecutorService getExecutor();

    /**
     * Reports evidence about an on-board copy of an entry, for example a verification report received in telemetry.
     * <p>
     * The engine resolves the reference to an entry (if several entries had the same reference over time, the one with
     * the release time closest to {@code time}). The state is only applied if it moves the entry forward. Evidence for
     * unknown references is logged and ignored.
     *
     * @param ref
     *            on-board reference
     * @param time
     *            when the evidence was produced (generation time of the telemetry)
     * @param source
     *            where the evidence comes from
     * @param state
     *            the state implied by the evidence, or null if the evidence does not imply a state
     * @param message
     *            optional detail, e.g. a failure code
     */
    void reportEvidence(String ref, long time, EvidenceSource source, EntryState state, String message);
}
