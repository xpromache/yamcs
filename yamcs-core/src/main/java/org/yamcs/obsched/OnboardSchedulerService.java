package org.yamcs.obsched;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.yamcs.ConfigurationException;
import org.yamcs.InitException;
import org.yamcs.Processor;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.ValidationException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.AbstractYamcsService;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.http.api.GpbWellKnownHelper;
import org.yamcs.mdb.Mdb;
import org.yamcs.obsched.OnboardSnapshot.OnboardItem;
import org.yamcs.obsched.SyncPlan.SyncStep;
import org.yamcs.protobuf.Commanding.CommandHistoryAttribute;
import org.yamcs.protobuf.obsched.EntryIntent;
import org.yamcs.protobuf.obsched.EntryOptionInfo;
import org.yamcs.protobuf.obsched.EntryState;
import org.yamcs.protobuf.obsched.ForeignItemInfo;
import org.yamcs.protobuf.obsched.PendingChangesInfo;
import org.yamcs.protobuf.obsched.RecordSource;
import org.yamcs.protobuf.obsched.SchedulerInfo;
import org.yamcs.protobuf.obsched.SyncInfo;
import org.yamcs.protobuf.obsched.SyncStatus;
import org.yamcs.protobuf.obsched.SyncStepInfo;
import org.yamcs.protobuf.obsched.SyncType;
import org.yamcs.security.User;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.xtce.MetaCommand;

import com.google.protobuf.Struct;

/**
 * Ground model of an on-board time-tagged command schedule.
 * <p>
 * Keeps the entries planned on the ground, synchronises them with the on-board schedule through a
 * {@link SchedulerAdapter} and records the execution status received in telemetry. One service instance manages one
 * on-board scheduler; the service name is the scheduler name.
 * <p>
 * See {@code docs/design/onboard-scheduler.md}.
 */
public class OnboardSchedulerService extends AbstractYamcsService implements SchedulerContext {

    /**
     * Command history attribute set on each upload command, with the entry id.
     */
    public static final String ATTR_ENTRY_ID = "onboardScheduleEntryId";
    static final String ACK_PREFIX = "OnboardSchedule_";

    private static final Map<String, String> ADAPTER_ALIASES = Map.of(
            "pus11", "org.yamcs.pus.Pus11SchedulerAdapter");

    public enum SyncMode {
        /** read the on-board schedule and reconcile the ground entries with it */
        READ_BACK,
        /** read back, then execute the pending changes */
        SYNC
    }

    private String processorName;
    private Processor processor;
    private SchedulerAdapter adapter;
    private ScheduleEntryDb db;
    private ScheduledExecutorService executor;
    private long stepTimeout;
    private long releaseTolerance;
    private long releaseWindow;

    private final Map<UUID, ScheduleEntry> entries = new LinkedHashMap<>();
    private final List<Consumer<ScheduleEntry>> listeners = new CopyOnWriteArrayList<>();
    private volatile SyncInfo lastSync;
    private boolean syncRunning;

    @Override
    public Spec getSpec() {
        var spec = new Spec();
        spec.addOption("processor", OptionType.STRING).withDefault("realtime")
                .withDescription("Processor used for commanding.");
        spec.addOption("adapter", OptionType.STRING).withRequired(true)
                .withDescription("Adapter type (pus11) or class name of a SchedulerAdapter implementation.");
        spec.addOption("adapterArgs", OptionType.MAP).withSpec(Spec.ANY).withDefault(new HashMap<>());
        spec.addOption("stepTimeout", OptionType.INTEGER).withDefault(60)
                .withDescription("Timeout in seconds for one sync step.");
        spec.addOption("releaseTolerance", OptionType.INTEGER).withDefault(5)
                .withDescription("Seconds of clock difference tolerated between the release time of an entry and the"
                        + " time of the telemetry reporting its release.");
        spec.addOption("releaseWindow", OptionType.INTEGER).withDefault(60)
                .withDescription("Seconds after the release time during which a release of the entry is expected."
                        + " Execution reports about the ref of an entry outside this window are attributed to"
                        + " another command with the same ref (counters wrap or restart) and ignored, unless the"
                        + " entry is still executing.");
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config) throws InitException {
        super.init(yamcsInstance, serviceName, config);
        processorName = config.getString("processor");
        stepTimeout = config.getInt("stepTimeout") * 1000L;
        releaseTolerance = config.getInt("releaseTolerance") * 1000L;
        releaseWindow = config.getInt("releaseWindow") * 1000L;
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "obsched-" + serviceName);
            t.setDaemon(true);
            return t;
        });

        var adapterName = config.getString("adapter");
        var className = ADAPTER_ALIASES.getOrDefault(adapterName, adapterName);
        try {
            adapter = (SchedulerAdapter) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException | ClassCastException e) {
            throw new ConfigurationException("Cannot instantiate scheduler adapter " + className + ": " + e);
        }
        YConfiguration adapterArgs;
        try {
            adapterArgs = adapter.getSpec().validate(config.getConfig("adapterArgs"));
        } catch (ValidationException e) {
            throw new InitException("Invalid adapterArgs: " + e.getMessage());
        }
        adapter.init(this, adapterArgs);

        db = new ScheduleEntryDb(yamcsInstance);
        long now = getMissionTime();
        for (var e : db.load(serviceName)) {
            entries.put(e.getId(), e);
            if (cancelDeletionIfReleased(e, now)) {
                db.save(e);
            }
        }
        log.debug("Loaded {} schedule entries", entries.size());
    }

    @Override
    protected void doStart() {
        processor = YamcsServer.getServer().getProcessor(yamcsInstance, processorName);
        if (processor == null) {
            notifyFailed(new ConfigurationException("No processor named '" + processorName + "'"));
            return;
        }
        adapter.start();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        adapter.stop();
        executor.shutdown();
        notifyStopped();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // SchedulerContext

    @Override
    public String getSchedulerName() {
        return serviceName;
    }

    @Override
    public Processor getProcessor() {
        return processor;
    }

    @Override
    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    @Override
    public synchronized void reportStatus(String ref, long time, RecordSource source, EntryState state,
            String message) {
        ScheduleEntry best = null;
        long bestDistance = Long.MAX_VALUE;
        boolean known = false;
        for (var e : entries.values()) {
            if (!e.hasRef(ref)) {
                continue;
            }
            known = true;
            if (!e.isPlausibleReport(ref, time, state, releaseTolerance, releaseWindow)) {
                continue;
            }
            long d = Math.abs(e.getReleaseTime() - time);
            if (d < bestDistance) {
                best = e;
                bestDistance = d;
            }
        }
        if (best == null) {
            if (known) {
                log.debug("Status {} for ref {} at {} ignored: does not fit the release of any entry with that ref",
                        state, ref, TimeEncoding.toString(time));
            } else {
                log.debug("Status {} for unknown ref {} ignored", state, ref);
            }
            return;
        }
        long now = getMissionTime();
        boolean changed = best.applyStatus(time, now, source, state, message);
        if (changed) {
            publishAck(best, ref, state, time, message);
        }
        cancelDeletionIfReleased(best, now);
        save(best);
    }

    /**
     * An entry deleted on the ground but released on board before a sync could remove it has run: its deletion is
     * cancelled, so that it shows as what happened on board.
     *
     * @return true if the entry was changed
     */
    private boolean cancelDeletionIfReleased(ScheduleEntry e, long now) {
        if (e.getIntent() != EntryIntent.DELETE_PENDING || !e.isReleased()) {
            return false;
        }
        e.setIntent(EntryIntent.PLANNED);
        e.addRecord(now, now, RecordSource.GROUND, null,
                "Released on board before its deletion could be synced: the deletion is cancelled");
        log.warn("Entry {} ({}) was released on board although deleted on the ground", e.getId(), e.getCommand());
        return true;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Ground model

    private static com.google.protobuf.Value toProtoValue(Object o) {
        if (o instanceof Number n) {
            return com.google.protobuf.Value.newBuilder().setNumberValue(n.doubleValue()).build();
        } else if (o instanceof Boolean bool) {
            return com.google.protobuf.Value.newBuilder().setBoolValue(bool).build();
        } else if (o instanceof String str) {
            return com.google.protobuf.Value.newBuilder().setStringValue(str).build();
        }
        return null;
    }

    public SchedulerInfo getSchedulerInfo() {
        var b = SchedulerInfo.newBuilder()
                .setName(serviceName)
                .setAdapter(adapter.getType())
                .setProcessor(processorName)
                .setCapabilities(adapter.getCapabilities().toProto())
                .setRefLabel(adapter.getRefLabel());
        if (adapter.getRefDescription() != null) {
            b.setRefDescription(adapter.getRefDescription());
        }
        var filterable = adapter.getFilterableOptions();
        for (var opt : adapter.getEntryOptionsSpec().getOptions()) {
            var ob = EntryOptionInfo.newBuilder().setName(opt.getName()).setType(opt.getType().name())
                    .setFilterable(filterable.contains(opt.getName()));
            if (opt.getDescription() != null && !opt.getDescription().isEmpty()) {
                ob.setDescription(String.join(" ", opt.getDescription()));
            }
            var defaultValue = toProtoValue(opt.getDefaultValue());
            if (defaultValue != null) {
                ob.setDefaultValue(defaultValue);
            }
            b.addEntryOptions(ob);
        }
        var ls = lastSync;
        if (ls != null) {
            b.setLastSync(ls);
        }
        return b.build();
    }

    public SchedulerAdapter getAdapter() {
        return adapter;
    }

    /**
     * @return copies of the entries released in [start, stop), sorted by release time
     */
    public synchronized List<ScheduleEntry> getEntries(long start, long stop, boolean includeDeleted) {
        var result = new ArrayList<ScheduleEntry>();
        for (var e : entries.values()) {
            if (!includeDeleted && e.getIntent() == EntryIntent.DELETED) {
                continue;
            }
            if (e.getReleaseTime() < start || e.getReleaseTime() >= stop) {
                continue;
            }
            result.add(e.copy());
        }
        result.sort(Comparator.comparingLong(ScheduleEntry::getReleaseTime));
        return result;
    }

    public synchronized ScheduleEntry getEntry(UUID id) {
        var e = entries.get(id);
        return e == null ? null : e.copy();
    }

    public synchronized ScheduleEntry createEntry(User user, String command, Struct args, long releaseTime,
            Struct options, String description, List<String> tags) throws InvalidEntryException {
        var e = new ScheduleEntry(UUID.randomUUID(), serviceName);
        e.setCommand(command);
        e.setArgs(args == null ? Struct.getDefaultInstance() : args);
        e.setReleaseTime(releaseTime);
        e.setOptions(options == null ? Struct.getDefaultInstance() : options);
        e.setDescription(description);
        if (tags != null) {
            e.setTags(tags);
        }
        validate(user, e);
        long now = getMissionTime();
        e.setCreated(now, user.getName());
        e.addRecord(now, now, RecordSource.GROUND, EntryState.NOT_ONBOARD, "Created by " + user.getName());
        entries.put(e.getId(), e);
        save(e);
        return e.copy();
    }

    /**
     * Updates an entry that is not on board. Null arguments are not changed.
     * <p>
     * An entry that is on board (or being uploaded) cannot be changed, it has to be
     * {@link #replaceEntry replaced}.
     */
    public synchronized ScheduleEntry updateEntry(User user, UUID id, String command, Struct args, Long releaseTime,
            Struct options, String description, List<String> tags) throws InvalidEntryException {
        var e = verifyEditable(id);
        if (e.getState() != EntryState.NOT_ONBOARD && e.getState() != EntryState.MISSED) {
            throw new InvalidEntryException("Entry cannot be changed in state " + e.getState()
                    + "; replace it by a new entry instead");
        }
        var candidate = e.copy();
        if (command != null) {
            candidate.setCommand(command);
        }
        if (args != null) {
            candidate.setArgs(args);
        }
        if (releaseTime != null) {
            candidate.setReleaseTime(releaseTime);
        }
        if (options != null) {
            candidate.setOptions(options);
        }
        validate(user, candidate);

        // only a change of what is uploaded requires a new revision (and a re-upload if on board)
        boolean onboardRelevant = !candidate.getCommand().equals(e.getCommand())
                || !candidate.getArgs().equals(e.getArgs())
                || candidate.getReleaseTime() != e.getReleaseTime()
                || !candidate.getOptions().equals(e.getOptions());
        if (command != null) {
            e.setCommand(command);
        }
        if (args != null) {
            e.setArgs(args);
        }
        if (releaseTime != null) {
            e.setReleaseTime(releaseTime);
        }
        if (options != null) {
            e.setOptions(options);
        }
        if (description != null) {
            e.setDescription(description);
        }
        if (tags != null) {
            e.setTags(tags);
        }
        long now = getMissionTime();
        e.setModified(now, user.getName());
        if (onboardRelevant) {
            e.incrementRevision();
            var msg = "Revision " + e.getRevision() + " by " + user.getName()
                    + (e.isOnboard() ? "; the on-board copy will be replaced at the next sync" : "");
            if (e.getState() == EntryState.MISSED) {
                e.forceState(now, RecordSource.GROUND, EntryState.NOT_ONBOARD, msg);
            } else {
                e.addRecord(now, now, RecordSource.GROUND, null, msg);
            }
        }
        save(e);
        return e.copy();
    }

    /**
     * Replaces an entry by a new one: the new entry is created and the old one deleted (removed from the on-board
     * schedule at the next sync if it is on board).
     *
     * @return the new entry
     */
    public synchronized ScheduleEntry replaceEntry(User user, UUID id, String command, Struct args, long releaseTime,
            Struct options, String description, List<String> tags) throws InvalidEntryException {
        var old = verifyEditable(id);
        var e = new ScheduleEntry(UUID.randomUUID(), serviceName);
        e.setCommand(command);
        e.setArgs(args == null ? Struct.getDefaultInstance() : args);
        e.setReleaseTime(releaseTime);
        e.setOptions(options == null ? Struct.getDefaultInstance() : options);
        e.setDescription(description);
        if (tags != null) {
            e.setTags(tags);
        }
        validate(user, e);
        long now = getMissionTime();
        e.setCreated(now, user.getName());
        e.addRecord(now, now, RecordSource.GROUND, EntryState.NOT_ONBOARD,
                "Created by " + user.getName() + " to replace entry " + old.getId());
        entries.put(e.getId(), e);
        save(e);
        markDeleted(user, old, "Replaced by entry " + e.getId() + " by " + user.getName());
        return e.copy();
    }

    public synchronized ScheduleEntry deleteEntry(User user, UUID id) throws InvalidEntryException {
        var e = verifyEditable(id);
        markDeleted(user, e, "Deleted by " + user.getName());
        return e.copy();
    }

    private void markDeleted(User user, ScheduleEntry e, String message) {
        long now = getMissionTime();
        e.setModified(now, user.getName());
        if (e.isOnboard()) {
            e.setIntent(EntryIntent.DELETE_PENDING);
            e.addRecord(now, now, RecordSource.GROUND, null,
                    message + "; will be removed from the on-board schedule at the next sync");
        } else {
            e.setIntent(EntryIntent.DELETED);
            e.addRecord(now, now, RecordSource.GROUND, null, message);
        }
        save(e);
    }

    private ScheduleEntry verifyEditable(UUID id) throws InvalidEntryException {
        var e = entries.get(id);
        if (e == null || e.getIntent() == EntryIntent.DELETED) {
            throw new InvalidEntryException("No entry " + id);
        }
        if (e.getIntent() == EntryIntent.DELETE_PENDING) {
            throw new InvalidEntryException("Entry is being deleted");
        }
        if (e.isReleased()) {
            throw new InvalidEntryException("Entry has already been released on board (" + e.getState() + ")");
        }
        return e;
    }

    private void validate(User user, ScheduleEntry e) throws InvalidEntryException {
        if (e.getCommand() == null || e.getCommand().isEmpty()) {
            throw new InvalidEntryException("Command is required");
        }
        if (e.getReleaseTime() == TimeEncoding.INVALID_INSTANT || e.getReleaseTime() == 0) {
            throw new InvalidEntryException("Release time is required");
        }
        try {
            adapter.getEntryOptionsSpec().validate(GpbWellKnownHelper.toJava(e.getOptions()));
        } catch (ValidationException ex) {
            throw new InvalidEntryException("Invalid options: " + ex.getMessage());
        }
        var mc = getMdb().getMetaCommand(e.getCommand());
        if (mc == null) {
            throw new InvalidEntryException("Unknown command " + e.getCommand());
        }
        // build the command to validate the arguments; the result is discarded
        try {
            processor.getCommandingManager().buildCommand(mc, GpbWellKnownHelper.toJava(e.getArgs()), "validation",
                    0, user);
        } catch (Exception ex) {
            throw new InvalidEntryException("Invalid command: " + ex.getMessage());
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Sync

    /**
     * Reads back the on-board schedule (if supported), reconciles the ground entries with it and, depending on the
     * mode, computes and executes the steps bringing the on-board schedule in line with the ground.
     */
    /**
     * Computes what the next sync would do, from the ground model only (the spacecraft is not contacted).
     */
    public synchronized PendingChangesInfo getPendingChanges() {
        long now = getMissionTime();
        long cutover = now + adapter.getCapabilities().minLeadTime();
        var plan = adapter.planSync(getEntries(Long.MIN_VALUE, Long.MAX_VALUE, false), null, cutover);
        var b = PendingChangesInfo.newBuilder()
                .setTime(TimeEncoding.toProtobufTimestamp(now))
                .addAllWarnings(plan.getWarnings());
        for (var step : plan.getSteps()) {
            b.addSteps(SyncStepInfo.newBuilder()
                    .setDescription(step.getDescription())
                    .addAllEntryIds(step.getEntryIds().stream().map(UUID::toString).toList()));
        }
        return b.build();
    }

    public CompletableFuture<SyncInfo> sync(User user, SyncMode mode) {
        var info = SyncInfo.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setStatus(SyncStatus.SYNC_RUNNING)
                .setType(mode == SyncMode.READ_BACK ? SyncType.READ_BACK : SyncType.SYNC)
                .setStart(TimeEncoding.toProtobufTimestamp(getMissionTime()))
                .setUser(user.getName());
        synchronized (this) {
            if (syncRunning) {
                return CompletableFuture.failedFuture(new IllegalStateException("A sync is already running"));
            }
            syncRunning = true;
        }
        var ctx = new SyncContextImpl(user);
        boolean readBack = adapter.getCapabilities().readBack();

        CompletableFuture<OnboardSnapshot> snapshotFuture;
        if (readBack) {
            snapshotFuture = adapter.readOnboard(ctx);
        } else if (mode == SyncMode.READ_BACK) {
            snapshotFuture = CompletableFuture.failedFuture(
                    new UnsupportedOperationException("The " + adapter.getType() + " adapter cannot read back"));
        } else {
            snapshotFuture = CompletableFuture.completedFuture(null);
        }

        var steps = new ArrayList<SyncStep>();
        var result = snapshotFuture.thenCompose(snapshot -> {
            List<ScheduleEntry> copies;
            synchronized (this) {
                if (snapshot != null) {
                    reconcile(snapshot, info);
                }
                if (mode == SyncMode.READ_BACK) {
                    return CompletableFuture.completedFuture(null);
                }
                long cutover = getMissionTime() + adapter.getCapabilities().minLeadTime();
                markMissed(cutover);
                copies = getEntries(Long.MIN_VALUE, Long.MAX_VALUE, false);
                var plan = adapter.planSync(copies, snapshot, cutover);
                info.addAllWarnings(plan.getWarnings());
                for (var step : plan.getSteps()) {
                    steps.add(step);
                    info.addSteps(SyncStepInfo.newBuilder()
                            .setDescription(step.getDescription())
                            .addAllEntryIds(step.getEntryIds().stream().map(UUID::toString).toList()));
                }
            }
            return executeSteps(ctx, steps, info, 0).thenCompose(v -> {
                if (!readBack || steps.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                // verify
                return adapter.readOnboard(ctx).thenAccept(snapshot2 -> {
                    synchronized (this) {
                        var verification = SyncInfo.newBuilder();
                        reconcile(snapshot2, verification);
                    }
                });
            });
        }).handle((v, err) -> {
            synchronized (this) {
                syncRunning = false;
                info.setStop(TimeEncoding.toProtobufTimestamp(getMissionTime()));
                if (err != null) {
                    var cause = unwrap(err);
                    log.warn("Sync failed: {}", cause.getMessage());
                    info.setStatus(SyncStatus.SYNC_FAILED).setFailureReason(String.valueOf(cause.getMessage()));
                } else {
                    info.setStatus(SyncStatus.SYNC_SUCCEEDED);
                }
                var built = info.build();
                lastSync = built;
                return built;
            }
        });
        return result;
    }

    private CompletableFuture<Void> executeSteps(SyncContext ctx, List<SyncStep> steps, SyncInfo.Builder info,
            int idx) {
        if (idx >= steps.size()) {
            return CompletableFuture.completedFuture(null);
        }
        var step = steps.get(idx);
        log.info("Sync step {}/{}: {}", idx + 1, steps.size(), step.getDescription());
        CompletableFuture<Void> f;
        try {
            f = step.execute(ctx);
        } catch (Exception e) {
            f = CompletableFuture.failedFuture(e);
        }
        return f.orTimeout(stepTimeout, TimeUnit.MILLISECONDS).handle((v, err) -> {
            synchronized (this) {
                var sb = info.getStepsBuilder(idx);
                if (err != null) {
                    var cause = unwrap(err);
                    sb.setError(cause instanceof java.util.concurrent.TimeoutException ? "timeout"
                            : String.valueOf(cause.getMessage()));
                } else {
                    sb.setDone(true);
                }
            }
            if (err != null) {
                throw new SyncStepException("Step " + (idx + 1) + " failed: " + step.getDescription(), unwrap(err));
            }
            return null;
        }).thenCompose(v -> executeSteps(ctx, steps, info, idx + 1));
    }

    /**
     * Reconciles the ground entries with the content of the on-board schedule. Must be called with the lock held.
     */
    private void reconcile(OnboardSnapshot snapshot, SyncInfo.Builder info) {
        long now = getMissionTime();
        var onboardRefs = new HashMap<String, OnboardItem>();
        for (var item : snapshot.items()) {
            onboardRefs.put(item.ref(), item);
        }
        var knownRefs = new HashSet<String>();

        for (var e : entries.values()) {
            if (e.getIntent() == EntryIntent.DELETED) {
                continue;
            }
            var ref = e.getRef();
            if (ref != null) {
                knownRefs.add(ref);
            }
            boolean changed = false;
            if (ref != null && onboardRefs.containsKey(ref)) {
                var s = e.getState();
                if (s == EntryState.UPLOAD_PENDING || s == EntryState.NOT_ONBOARD || s == EntryState.UNKNOWN) {
                    e.forceState(now, RecordSource.READBACK, EntryState.ONBOARD, "Found in the on-board schedule");
                    changed = true;
                }
            } else if (e.isOnboard()) {
                if (e.getReleaseTime() <= snapshot.time()) {
                    e.forceState(now, RecordSource.READBACK, EntryState.UNKNOWN,
                            "Not in the on-board schedule after its release time and no execution report");
                } else {
                    e.forceState(now, RecordSource.READBACK, EntryState.NOT_ONBOARD,
                            "Missing from the on-board schedule");
                }
                changed = true;
                if (e.getIntent() == EntryIntent.DELETE_PENDING && !e.isOnboard()) {
                    e.setIntent(EntryIntent.DELETED);
                    e.addRecord(now, now, RecordSource.READBACK, null, "Deletion confirmed by read-back");
                }
            }
            if (changed) {
                save(e);
            }
        }
        // the refs of older copies of an entry are not foreign, but stale: the adapter cleans them up
        for (var item : snapshot.items()) {
            if (knownRefs.contains(item.ref())) {
                continue;
            }
            var stale = entries.values().stream().filter(e -> e.hasRef(item.ref())).findFirst();
            var fb = ForeignItemInfo.newBuilder()
                    .setRef(item.ref())
                    .setReleaseTime(TimeEncoding.toProtobufTimestamp(item.releaseTime()));
            if (stale.isPresent()) {
                fb.setDescription("Stale copy of entry " + stale.get().getId());
            } else if (item.description() != null) {
                fb.setDescription(item.description());
            }
            info.addForeignItems(fb);
        }
    }

    private void markMissed(long cutover) {
        long now = getMissionTime();
        for (var e : entries.values()) {
            if (e.getIntent() == EntryIntent.PLANNED && e.getState() == EntryState.NOT_ONBOARD
                    && e.getReleaseTime() < cutover) {
                e.forceState(now, RecordSource.GROUND, EntryState.MISSED,
                        "Not uploaded before its release time");
                save(e);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    public void addEntryListener(Consumer<ScheduleEntry> listener) {
        listeners.add(listener);
    }

    public void removeEntryListener(Consumer<ScheduleEntry> listener) {
        listeners.remove(listener);
    }

    private void save(ScheduleEntry e) {
        db.save(e);
        if (!listeners.isEmpty()) {
            var copy = e.copy();
            for (var l : listeners) {
                try {
                    l.accept(copy);
                } catch (Exception ex) {
                    log.warn("Entry listener failed", ex);
                }
            }
        }
    }

    private void publishAck(ScheduleEntry e, String ref, EntryState state, long time, String message) {
        var cmdIdStr = e.getUploadCommandId(ref);
        if (cmdIdStr == null || processor == null) {
            return;
        }
        String key;
        AckStatus status = AckStatus.OK;
        switch (state) {
        case ONBOARD -> key = "Inserted";
        case RELEASED -> key = "Released";
        case STARTED -> key = "Started";
        case COMPLETED -> key = "Completed";
        case FAILED -> {
            key = "Completed";
            status = AckStatus.NOK;
        }
        default -> {
            return;
        }
        }
        try {
            var cmdId = CommandIds.fromStringIdentifier(e.getCommand(), cmdIdStr);
            processor.getCommandHistoryPublisher().publishAck(cmdId, ACK_PREFIX + key, time, status, message);
        } catch (Exception ex) {
            log.warn("Cannot publish command history ack for entry {}: {}", e.getId(), ex.toString());
        }
    }

    private Mdb getMdb() {
        return processor.getMdb();
    }

    private long getMissionTime() {
        return processor != null ? processor.getCurrentTime() : TimeEncoding.getWallclockTime();
    }

    private static Throwable unwrap(Throwable t) {
        while ((t instanceof java.util.concurrent.CompletionException
                || t instanceof java.util.concurrent.ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    private static String origin() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "obsched";
        }
    }

    @SuppressWarnings("serial")
    static class SyncStepException extends RuntimeException {
        SyncStepException(String msg, Throwable cause) {
            super(msg + ": " + cause.getMessage(), cause);
        }
    }

    class SyncContextImpl implements SyncContext {
        final User user;

        SyncContextImpl(User user) {
            this.user = user;
        }

        @Override
        public User getUser() {
            return user;
        }

        @Override
        public IssuedCommand sendCommand(String command, Map<String, Object> args, Map<String, Object> options) {
            var mc = getMdb().getMetaCommand(command);
            if (mc == null) {
                throw new IllegalArgumentException("Unknown command " + command);
            }
            var pc = build(mc, args, options);
            return send(pc);
        }

        @Override
        public IssuedCommand uploadEntry(UUID entryId, Map<String, Object> options) {
            ScheduleEntry e;
            synchronized (OnboardSchedulerService.this) {
                e = entries.get(entryId);
                if (e == null) {
                    throw new IllegalArgumentException("No entry " + entryId);
                }
            }
            var mc = getMdb().getMetaCommand(e.getCommand());
            if (mc == null) {
                throw new IllegalArgumentException("Unknown command " + e.getCommand());
            }
            var pc = build(mc, GpbWellKnownHelper.toJava(e.getArgs()), options);
            pc.disableCommandVerifiers(true);
            pc.addAttribute(CommandHistoryAttribute.newBuilder().setName(ATTR_ENTRY_ID)
                    .setValue(org.yamcs.protobuf.Yamcs.Value.newBuilder()
                            .setType(org.yamcs.protobuf.Yamcs.Value.Type.STRING)
                            .setStringValue(e.getId().toString()))
                    .build());
            synchronized (OnboardSchedulerService.this) {
                long now = getMissionTime();
                e.addUpload(CommandIds.toStringIdentifier(pc.getCommandId()), now);
                e.setRef(null);
                e.setUploadedRevision(e.getRevision());
                e.forceState(now, RecordSource.GROUND, EntryState.UPLOAD_PENDING,
                        "Upload sent (revision " + e.getRevision() + ")");
                save(e);
            }
            return send(pc);
        }

        @Override
        public void setUploadRef(UUID entryId, IssuedCommand upload, String ref) {
            synchronized (OnboardSchedulerService.this) {
                var e = entries.get(entryId);
                if (e != null) {
                    e.setUploadRef(upload.getId(), ref);
                    save(e);
                }
            }
        }

        @Override
        public void confirmDeleted(UUID entryId, String ref) {
            synchronized (OnboardSchedulerService.this) {
                var e = entries.get(entryId);
                if (e == null) {
                    return;
                }
                long now = getMissionTime();
                if (ref.equals(e.getRef())) {
                    if (e.getIntent() == EntryIntent.DELETE_PENDING) {
                        e.setIntent(EntryIntent.DELETED);
                        e.forceState(now, RecordSource.GROUND, EntryState.NOT_ONBOARD,
                                "Deleted from the on-board schedule");
                    } else {
                        e.forceState(now, RecordSource.GROUND, EntryState.NOT_ONBOARD,
                                "Old copy deleted from the on-board schedule");
                    }
                } else {
                    e.addRecord(now, now, RecordSource.GROUND, null,
                            "Stale copy " + ref + " deleted from the on-board schedule");
                }
                var cmdIdStr = e.getUploadCommandId(ref);
                if (cmdIdStr != null) {
                    try {
                        var cmdId = CommandIds.fromStringIdentifier(e.getCommand(), cmdIdStr);
                        processor.getCommandHistoryPublisher().publishAck(cmdId, ACK_PREFIX + "Deleted", now,
                                AckStatus.OK);
                    } catch (Exception ex) {
                        log.warn("Cannot publish command history ack: {}", ex.toString());
                    }
                }
                save(e);
            }
        }

        @Override
        public void uploadFailed(UUID entryId, String reason) {
            synchronized (OnboardSchedulerService.this) {
                var e = entries.get(entryId);
                if (e == null || e.getState() != EntryState.UPLOAD_PENDING) {
                    return;
                }
                long now = getMissionTime();
                e.forceState(now, RecordSource.GROUND, EntryState.NOT_ONBOARD, "Upload failed: " + reason);
                var upload = e.getLatestUpload();
                if (upload != null) {
                    try {
                        var cmdId = CommandIds.fromStringIdentifier(e.getCommand(), upload.getCommandId());
                        processor.getCommandHistoryPublisher().publishAck(cmdId, ACK_PREFIX + "Inserted", now,
                                AckStatus.NOK, reason);
                    } catch (Exception ex) {
                        log.warn("Cannot publish command history ack: {}", ex.toString());
                    }
                }
                save(e);
            }
        }

        private PreparedCommand build(MetaCommand mc, Map<String, Object> args, Map<String, Object> options) {
            PreparedCommand pc;
            try {
                pc = processor.getCommandingManager().buildCommand(mc, args, origin(), 0, user);
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot build command " + mc.getQualifiedName() + ": "
                        + e.getMessage(), e);
            }
            if (options != null) {
                options.forEach((k, v) -> {
                    var commandOption = YamcsServer.getServer().getCommandOption(k);
                    if (commandOption == null) {
                        throw new IllegalArgumentException("Unknown command option '" + k + "'");
                    }
                    pc.addAttribute(CommandHistoryAttribute.newBuilder()
                            .setName(k)
                            .setValue(commandOption.coerceValue(v))
                            .build());
                });
            }
            return pc;
        }

        private IssuedCommand send(PreparedCommand pc) {
            var issued = new IssuedCommand(pc.getCommandId());
            var cm = processor.getCommandingManager();
            cm.sendCommand(user, pc);
            try {
                var existing = processor.getCommandHistoryManager().subscribeCommand(pc.getCommandId(), issued);
                for (var a : existing.getAttrList()) {
                    issued.update(a.getName(), org.yamcs.utils.ValueUtility.fromGpb(a.getValue()));
                }
            } catch (Exception e) {
                log.warn("Cannot subscribe to the command history of {}: {}", pc.getCommandId(), e.toString());
            }
            return issued;
        }
    }
}
