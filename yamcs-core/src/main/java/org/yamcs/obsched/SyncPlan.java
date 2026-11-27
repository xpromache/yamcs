package org.yamcs.obsched;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The steps that an adapter proposes to bring the on-board schedule in line with the ground.
 */
public class SyncPlan {

    /**
     * One step of a sync, for example a command deleting a batch of activities.
     */
    public interface SyncStep {
        /**
         * Human readable description, shown in the dry run.
         */
        String getDescription();

        /**
         * The entries affected by the step.
         */
        List<UUID> getEntryIds();

        /**
         * Executes the step. The returned future completes when the step has been executed (normally when the
         * on-board acknowledgement has been received) and completes exceptionally if it failed.
         */
        CompletableFuture<Void> execute(SyncContext ctx);
    }

    private final List<SyncStep> steps = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();

    public void addStep(SyncStep step) {
        steps.add(step);
    }

    public void addWarning(String warning) {
        warnings.add(warning);
    }

    public List<SyncStep> getSteps() {
        return steps;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public boolean isEmpty() {
        return steps.isEmpty();
    }
}
