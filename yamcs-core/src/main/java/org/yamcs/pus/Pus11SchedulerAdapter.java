package org.yamcs.pus;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.yamcs.ConfigurationException;
import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.YConfiguration;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.http.api.GpbWellKnownHelper;
import org.yamcs.logging.Log;
import org.yamcs.obsched.IssuedCommand;
import org.yamcs.obsched.OnboardSnapshot;
import org.yamcs.obsched.OnboardSnapshot.OnboardItem;
import org.yamcs.obsched.ScheduleEntry;
import org.yamcs.obsched.SchedulerAdapter;
import org.yamcs.obsched.SchedulerCapabilities;
import org.yamcs.obsched.SchedulerContext;
import org.yamcs.obsched.SyncContext;
import org.yamcs.obsched.SyncPlan;
import org.yamcs.obsched.SyncPlan.SyncStep;
import org.yamcs.protobuf.obsched.EntryIntent;
import org.yamcs.protobuf.obsched.EntryState;
import org.yamcs.protobuf.obsched.EvidenceSource;
import org.yamcs.tctm.ccsds.time.CucTimeDecoder;
import org.yamcs.utils.ByteArrayUtils;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;

/**
 * {@link SchedulerAdapter} for the PUS ST[11] time-based scheduling service.
 * <p>
 * <ul>
 * <li>Entries are inserted one by one by sending the entry command with the {@code pus11ScheduleAt} option, which makes
 * the {@link PusCommandPostprocessor} wrap it into a TC[11,4]. The post-processor must therefore have the {@code pus11}
 * block configured.</li>
 * <li>Entries are deleted with TC[11,5] (delete by request identifier).</li>
 * <li>The on-board schedule is read back with TC[11,17] (summary report all), answered by TM[11,13].</li>
 * <li>Execution evidence comes from the PUS ST[1] verification reports of the released TCs, received in realtime or
 * in dumps of on-board stored TM.</li>
 * </ul>
 * The on-board reference of an entry is {@code apid/seqCount} of the scheduled TC: the PUS ST[1] reports identify the
 * TC by these two fields only.
 */
public class Pus11SchedulerAdapter implements SchedulerAdapter {

    static final String OPT_SUB_SCHEDULE = "subScheduleId";
    static final String OPT_GROUP = "groupId";

    private Log log;
    private SchedulerContext ctx;

    private String realtimeStreamName;
    private List<String> dumpStreamNames;
    private int tmUserDataOffset;
    private String deleteCommand;
    private String summaryReportCommand;
    private int maxDeletesPerCommand;
    private int reportCountBytes;
    private int subScheduleIdBytes;
    private int groupIdBytes;
    private Integer defaultSubScheduleId;
    private Integer defaultGroupId;
    private CucTimeDecoder timeDecoder;
    private String epoch;
    private long minLeadTime;
    private long ackTimeout;
    private int defaultSourceId;

    private final List<Runnable> unsubscribers = new ArrayList<>();

    // apid/seq of a TC sent by the adapter -> its pending completion (TM[1,7]) (only realtime TM)
    private final Map<String, CompletableFuture<Void>> pendingCompletions = new HashMap<>();
    // recently seen verification reports on the realtime stream: ref -> null (success) or failure message.
    // Covers the race between the report and the command history giving us the sequence count.
    private final Map<String, String> recentCompletions = new LinkedHashMap<>(256, 0.75f, false) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > 1000;
        }
    };

    // outer TC[11,4] ref -> (entry id, inner ref)
    private final Map<String, PendingInsert> pendingInserts = new HashMap<>();
    // summary report collection in progress
    private List<OnboardItem> collectingReport;
    private long collectingReportTime;
    // ref -> source id, learned from the summary reports (needed to build the TC[11,5] request ids)
    private final Map<String, Integer> sourceIds = new HashMap<>();

    record PendingInsert(UUID entryId, String innerRef) {
    }

    @Override
    public Spec getSpec() {
        var spec = new Spec();
        spec.addOption("realtimeStream", OptionType.STRING).withDefault("tm_realtime")
                .withDescription("Stream with the realtime TM");
        spec.addOption("dumpStreams", OptionType.LIST).withElementType(OptionType.STRING)
                .withDefault(List.of("tm_dump"))
                .withDescription("Streams with TM recorded on board and dumped; missing streams are ignored");
        spec.addOption("tmUserDataOffset", OptionType.INTEGER).withDefault(21)
                .withDescription("Offset of the user data in the TM packets (primary + secondary header length)");
        spec.addOption("deleteCommand", OptionType.STRING).withDefault("/PUS11/DELETE_ACTIVITIES_BY_ID");
        spec.addOption("summaryReportCommand", OptionType.STRING).withDefault("/PUS11/GET_SUMMARY_REPORT");
        spec.addOption("maxDeletesPerCommand", OptionType.INTEGER).withDefault(32);
        spec.addOption("reportCountBytes", OptionType.INTEGER).withDefault(2)
                .withDescription("Size of the N field in the TM[11,13]");
        spec.addOption("subScheduleIdBytes", OptionType.INTEGER).withDefault(1)
                .withDescription("Size of the sub-schedule id in the TM[11,13] (0 if absent)");
        spec.addOption("groupIdBytes", OptionType.INTEGER).withDefault(1)
                .withDescription("Size of the group id in the TM[11,13] (0 if absent)");
        spec.addOption("defaultSubScheduleId", OptionType.INTEGER)
                .withDescription("Sub-schedule of the entries that do not set one. Should match the default of the"
                        + " command post-processor; if set, it is passed explicitly with each insert");
        spec.addOption("defaultGroupId", OptionType.INTEGER)
                .withDescription("Group of the entries that do not set one. Should match the default of the"
                        + " command post-processor; if set, it is passed explicitly with each insert");
        spec.addOption("pfield", OptionType.INTEGER).withDefault(-1)
                .withDescription("CUC p-field of the release times in the TM[11,13]; -1 if the p-field is explicit");
        spec.addOption("epoch", OptionType.STRING).withDefault("UNIX").withChoices("UNIX", "GPS", "TAI", "J2000");
        spec.addOption("minLeadTime", OptionType.INTEGER).withDefault(10)
                .withDescription("Seconds between the upload and the release time below which an entry is not uploaded");
        spec.addOption("ackTimeout", OptionType.INTEGER).withDefault(30)
                .withDescription("Seconds to wait for the on-board completion of a TC");
        spec.addOption("sourceId", OptionType.INTEGER).withDefault(0)
                .withDescription("Source id used in the TC[11,5] request ids when not known from a read-back");
        return spec;
    }

    @Override
    public void init(SchedulerContext ctx, YConfiguration config) throws InitException {
        this.ctx = ctx;
        this.log = new Log(getClass(), ctx.getYamcsInstance());
        log.setContext(ctx.getSchedulerName());
        realtimeStreamName = config.getString("realtimeStream");
        dumpStreamNames = config.getList("dumpStreams");
        tmUserDataOffset = config.getInt("tmUserDataOffset");
        deleteCommand = config.getString("deleteCommand");
        summaryReportCommand = config.getString("summaryReportCommand");
        maxDeletesPerCommand = config.getInt("maxDeletesPerCommand");
        reportCountBytes = config.getInt("reportCountBytes");
        subScheduleIdBytes = config.getInt("subScheduleIdBytes");
        groupIdBytes = config.getInt("groupIdBytes");
        defaultSubScheduleId = config.containsKey("defaultSubScheduleId") ? config.getInt("defaultSubScheduleId")
                : null;
        defaultGroupId = config.containsKey("defaultGroupId") ? config.getInt("defaultGroupId") : null;
        timeDecoder = new CucTimeDecoder(config.getInt("pfield"));
        epoch = config.getString("epoch");
        minLeadTime = config.getInt("minLeadTime") * 1000L;
        ackTimeout = config.getInt("ackTimeout") * 1000L;
        defaultSourceId = config.getInt("sourceId");
    }

    @Override
    public String getType() {
        return "pus11";
    }

    @Override
    public SchedulerCapabilities getCapabilities() {
        return new SchedulerCapabilities(true, true,
                EnumSet.of(EntryState.UPLOAD_PENDING, EntryState.ONBOARD, EntryState.RELEASED, EntryState.STARTED,
                        EntryState.COMPLETED, EntryState.FAILED, EntryState.UNKNOWN),
                minLeadTime);
    }

    @Override
    public Spec getEntryOptionsSpec() {
        // an option is only offered if the TCs carry the field
        var spec = new Spec();
        if (subScheduleIdBytes > 0) {
            var opt = spec.addOption(OPT_SUB_SCHEDULE, OptionType.INTEGER).withDescription("Sub-schedule id");
            if (defaultSubScheduleId != null) {
                opt.withDefault(defaultSubScheduleId);
            }
        }
        if (groupIdBytes > 0) {
            var opt = spec.addOption(OPT_GROUP, OptionType.INTEGER).withDescription("Scheduling group id");
            if (defaultGroupId != null) {
                opt.withDefault(defaultGroupId);
            }
        }
        return spec;
    }

    @Override
    public Set<String> getFilterableOptions() {
        return Set.of(OPT_SUB_SCHEDULE, OPT_GROUP);
    }

    @Override
    public void start() {
        var ydb = YarchDatabase.getInstance(ctx.getYamcsInstance());
        subscribe(ydb.getStream(realtimeStreamName), realtimeStreamName, EvidenceSource.REALTIME, true);
        for (var name : dumpStreamNames) {
            subscribe(ydb.getStream(name), name, EvidenceSource.DUMP, false);
        }
    }

    private void subscribe(Stream stream, String name, EvidenceSource source, boolean realtime) {
        if (stream == null) {
            if (realtime) {
                throw new ConfigurationException("Stream " + name + " does not exist");
            }
            log.info("Dump stream {} does not exist, ignoring", name);
            return;
        }
        var subscriber = new StreamSubscriber() {
            @Override
            public void onTuple(Stream s, Tuple tuple) {
                try {
                    processPacket(tuple, source, realtime);
                } catch (Exception e) {
                    log.warn("Error processing packet from {}", name, e);
                }
            }
        };
        stream.addSubscriber(subscriber);
        unsubscribers.add(() -> stream.removeSubscriber(subscriber));
    }

    @Override
    public void stop() {
        unsubscribers.forEach(Runnable::run);
        unsubscribers.clear();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Telemetry

    void processPacket(Tuple tuple, EvidenceSource source, boolean realtime) {
        byte[] packet = tuple.getColumn(StandardTupleDefinitions.TM_PACKET_COLUMN);
        long gentime = tuple.getTimestampColumn(StandardTupleDefinitions.GENTIME_COLUMN);
        if (packet.length < tmUserDataOffset || (packet[0] & 0x08) == 0) { // no secondary header
            return;
        }
        int type = packet[7] & 0xFF;
        int subtype = packet[8] & 0xFF;
        if (type == 1) {
            processVerificationReport(packet, subtype, gentime, source, realtime);
        } else if (type == 11 && subtype == 13 && realtime) {
            processSummaryReport(packet, gentime);
        }
    }

    private void processVerificationReport(byte[] packet, int subtype, long gentime, EvidenceSource source,
            boolean realtime) {
        int offset = tmUserDataOffset;
        if (packet.length < offset + 4) {
            return;
        }
        int apid = ByteArrayUtils.decodeUnsignedShort(packet, offset) & 0x07FF;
        int seq = ByteArrayUtils.decodeUnsignedShort(packet, offset + 2) & 0x3FFF;
        var ref = ref(apid, seq);
        boolean failure = (subtype % 2) == 0;
        String failureMessage = null;
        if (failure) {
            failureMessage = "TM[1," + subtype + "] " + stageName(subtype) + " failure";
            if (packet.length >= offset + 8) {
                failureMessage += ", code " + ByteArrayUtils.decodeInt(packet, offset + 4);
            }
        }

        if (realtime) {
            PendingInsert pendingInsert;
            CompletableFuture<Void> pending = null;
            synchronized (this) {
                pendingInsert = subtype == 7 || failure ? pendingInserts.remove(ref) : pendingInserts.get(ref);
                if (subtype == 7 || failure) {
                    pending = pendingCompletions.remove(ref);
                    recentCompletions.put(ref, failure ? failureMessage : "");
                }
            }
            if (pendingInsert != null) {
                // report about the TC[11,4] carrying the entry, not about the entry itself
                if (subtype == 7) {
                    ctx.reportEvidence(pendingInsert.innerRef(), gentime, source, EntryState.ONBOARD,
                            "Inserted in the on-board schedule");
                }
            }
            if (pending != null) {
                if (failure) {
                    pending.completeExceptionally(new IllegalStateException(failureMessage));
                } else {
                    pending.complete(null);
                }
            }
            if (pendingInsert != null) {
                return;
            }
        }

        EntryState state = switch (subtype) {
        case 1 -> EntryState.RELEASED;
        case 3, 5 -> EntryState.STARTED;
        case 7 -> EntryState.COMPLETED;
        case 2, 4, 6, 8 -> EntryState.FAILED;
        default -> null;
        };
        if (state != null) {
            ctx.reportEvidence(ref, gentime, source, state,
                    failure ? failureMessage : "TM[1," + subtype + "] " + stageName(subtype));
        }
    }

    private static String stageName(int subtype) {
        return switch (subtype) {
        case 1, 2 -> "acceptance";
        case 3, 4 -> "start of execution";
        case 5, 6 -> "progress of execution";
        case 7, 8 -> "completion of execution";
        default -> "verification";
        };
    }

    private void processSummaryReport(byte[] packet, long gentime) {
        List<OnboardItem> collecting;
        synchronized (this) {
            collecting = collectingReport;
            collectingReportTime = gentime;
        }
        if (collecting == null) {
            return;
        }
        int offset = tmUserDataOffset;
        long n = decodeUnsigned(packet, offset, reportCountBytes);
        offset += reportCountBytes;
        var items = new ArrayList<OnboardItem>();
        for (int i = 0; i < n; i++) {
            long subSchedule = decodeUnsigned(packet, offset, subScheduleIdBytes);
            offset += subScheduleIdBytes;
            long group = decodeUnsigned(packet, offset, groupIdBytes);
            offset += groupIdBytes;
            var supplier = new int[] { offset };
            long t = timeDecoder.decode(() -> packet[supplier[0]++]);
            offset = supplier[0];
            long releaseTime = toYamcsTime(t);
            int sourceId = ByteArrayUtils.decodeUnsignedShort(packet, offset);
            int apid = ByteArrayUtils.decodeUnsignedShort(packet, offset + 2) & 0x07FF;
            int seq = ByteArrayUtils.decodeUnsignedShort(packet, offset + 4) & 0x3FFF;
            offset += 6;
            var ref = ref(apid, seq);
            items.add(new OnboardItem(ref, releaseTime,
                    "sub-schedule " + subSchedule + ", group " + group + ", source " + sourceId));
            synchronized (this) {
                sourceIds.put(ref, sourceId);
            }
        }
        synchronized (this) {
            collecting.addAll(items);
        }
    }

    private long toYamcsTime(long t) {
        return switch (epoch) {
        case "GPS" -> TimeEncoding.fromGpsMillisec(t);
        case "TAI" -> TimeEncoding.fromTaiMillisec(t);
        case "J2000" -> TimeEncoding.fromJ2000Millisec(t);
        default -> TimeEncoding.fromUnixMillisec(t);
        };
    }

    private static long decodeUnsigned(byte[] b, int offset, int n) {
        long x = 0;
        for (int i = 0; i < n; i++) {
            x = (x << 8) | (b[offset + i] & 0xFF);
        }
        return x;
    }

    static String ref(int apid, int seq) {
        return apid + "/" + seq;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Read-back

    @Override
    public CompletableFuture<OnboardSnapshot> readOnboard(SyncContext sctx) {
        var items = new ArrayList<OnboardItem>();
        synchronized (this) {
            if (collectingReport != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("A read-back is already running"));
            }
            collectingReport = items;
            collectingReportTime = TimeEncoding.INVALID_INSTANT;
        }
        IssuedCommand cmd;
        try {
            cmd = sctx.sendCommand(summaryReportCommand, Map.of(), Map.of());
        } catch (RuntimeException e) {
            synchronized (this) {
                collectingReport = null;
            }
            return CompletableFuture.failedFuture(e);
        }
        return awaitOnboardCompletion(cmd).handle((v, err) -> {
            long time;
            synchronized (this) {
                collectingReport = null;
                time = collectingReportTime;
            }
            if (err != null) {
                throw new IllegalStateException(
                        "Read-back of the on-board schedule failed: " + unwrap(err).getMessage(), err);
            }
            if (time == TimeEncoding.INVALID_INSTANT) {
                // no TM[11,13] received; the completion report came, so the schedule is reported empty only if the
                // spacecraft did not send anything: be conservative
                throw new IllegalStateException("No TM[11,13] received in response to " + summaryReportCommand);
            }
            log.debug("Read back {} on-board activities", items.size());
            return new OnboardSnapshot(time, List.copyOf(items));
        });
    }

    /**
     * Waits for the TM[1,7] of a TC sent by the adapter.
     */
    private CompletableFuture<Void> awaitOnboardCompletion(IssuedCommand cmd) {
        return refOf(cmd).thenCompose(ref -> {
            synchronized (this) {
                if (recentCompletions.containsKey(ref)) {
                    var failure = recentCompletions.remove(ref);
                    return failure.isEmpty() ? CompletableFuture.completedFuture(null)
                            : CompletableFuture.failedFuture(new IllegalStateException(failure));
                }
                var f = new CompletableFuture<Void>();
                pendingCompletions.put(ref, f);
                return f;
            }
        }).orTimeout(ackTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * The apid/seq of a TC sent by the adapter, from its command history.
     */
    private static CompletableFuture<String> refOf(IssuedCommand cmd) {
        var seqF = cmd.attribute(CommandHistoryPublisher.CcsdsSeq_KEY);
        var binF = cmd.attribute(PreparedCommand.CNAME_BINARY);
        return seqF.thenCombine(binF, (seq, bin) -> {
            int apid = ByteArrayUtils.decodeUnsignedShort(bin.getBinaryValue(), 0) & 0x07FF;
            return ref(apid, seq.getSint32Value() & 0x3FFF);
        });
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Sync

    @Override
    public SyncPlan planSync(List<ScheduleEntry> entries, OnboardSnapshot snapshot, long cutover) {
        var plan = new SyncPlan();

        // deletions: entries to delete, old copies of modified entries, stale copies
        var deletions = new ArrayList<Deletion>();
        for (var e : entries) {
            if (!e.isOnboard() || e.getRef() == null) {
                continue;
            }
            if (e.getIntent() == EntryIntent.DELETE_PENDING) {
                deletions.add(new Deletion(e.getId(), e.getRef(), "delete"));
            } else if (e.isModifiedSinceUpload()) {
                deletions.add(new Deletion(e.getId(), e.getRef(), "replace with revision " + e.getRevision()));
            }
        }
        if (snapshot != null) {
            for (var item : snapshot.items()) {
                for (var e : entries) {
                    if (e.hasRef(item.ref()) && !item.ref().equals(e.getRef())) {
                        deletions.add(new Deletion(e.getId(), item.ref(), "stale copy"));
                    }
                }
            }
        }
        for (int i = 0; i < deletions.size(); i += maxDeletesPerCommand) {
            var batch = deletions.subList(i, Math.min(deletions.size(), i + maxDeletesPerCommand));
            plan.addStep(new DeleteStep(List.copyOf(batch)));
        }

        // insertions
        for (var e : entries) {
            if (e.getIntent() != EntryIntent.PLANNED) {
                continue;
            }
            boolean needsUpload = e.getState() == EntryState.NOT_ONBOARD
                    || (e.isOnboard() && e.isModifiedSinceUpload());
            if (!needsUpload) {
                continue;
            }
            if (e.getReleaseTime() < cutover) {
                if (e.isOnboard()) {
                    plan.addWarning("Entry " + e.getId() + " (" + e.getCommand() + ") was modified but is due before "
                            + TimeEncoding.toString(cutover) + "; the old revision stays on board");
                }
                continue;
            }
            plan.addStep(new InsertStep(e));
        }
        return plan;
    }

    record Deletion(UUID entryId, String ref, String reason) {
    }

    class DeleteStep implements SyncStep {
        final List<Deletion> deletions;

        DeleteStep(List<Deletion> deletions) {
            this.deletions = deletions;
        }

        @Override
        public String getDescription() {
            var sb = new StringBuilder("TC[11,5] delete " + deletions.size() + " activit"
                    + (deletions.size() == 1 ? "y" : "ies") + ":");
            for (var d : deletions) {
                sb.append(" ").append(d.ref()).append(" (").append(d.reason()).append(")");
            }
            return sb.toString();
        }

        @Override
        public List<UUID> getEntryIds() {
            return deletions.stream().map(Deletion::entryId).distinct().toList();
        }

        @Override
        public CompletableFuture<Void> execute(SyncContext sctx) {
            var requests = new ArrayList<Map<String, Object>>();
            for (var d : deletions) {
                var parts = d.ref().split("/");
                int sourceId;
                synchronized (Pus11SchedulerAdapter.this) {
                    sourceId = sourceIds.getOrDefault(d.ref(), defaultSourceId);
                }
                requests.add(Map.of(
                        "source_id", sourceId,
                        "apid", Integer.parseInt(parts[0]),
                        "seqcount", Integer.parseInt(parts[1])));
            }
            var args = new HashMap<String, Object>();
            args.put("num_requests", requests.size());
            args.put("requests", requests);
            var cmd = sctx.sendCommand(deleteCommand, args, Map.of());
            return awaitOnboardCompletion(cmd).thenRun(() -> {
                for (var d : deletions) {
                    sctx.confirmDeleted(d.entryId(), d.ref());
                }
            });
        }
    }

    class InsertStep implements SyncStep {
        final ScheduleEntry entry;

        InsertStep(ScheduleEntry entry) {
            this.entry = entry;
        }

        @Override
        public String getDescription() {
            return "TC[11,4] insert " + entry.getCommand() + " at " + TimeEncoding.toString(entry.getReleaseTime())
                    + " (revision " + entry.getRevision() + ")";
        }

        @Override
        public List<UUID> getEntryIds() {
            return List.of(entry.getId());
        }

        @Override
        public CompletableFuture<Void> execute(SyncContext sctx) {
            var options = new HashMap<String, Object>();
            options.put(PusCommandPostprocessor.OPTION_SCHEDULE_TIME.getId(), entry.getReleaseTime());
            var entryOptions = GpbWellKnownHelper.toJava(entry.getOptions());
            if (entryOptions.get(OPT_SUB_SCHEDULE) instanceof Number n) {
                options.put(PusCommandPostprocessor.OPTION_SUB_SCHEDULE_ID.getId(), n.doubleValue());
            } else if (defaultSubScheduleId != null && subScheduleIdBytes > 0) {
                options.put(PusCommandPostprocessor.OPTION_SUB_SCHEDULE_ID.getId(), defaultSubScheduleId.doubleValue());
            }
            if (entryOptions.get(OPT_GROUP) instanceof Number n) {
                options.put(PusCommandPostprocessor.OPTION_GROUP_ID.getId(), n.doubleValue());
            } else if (defaultGroupId != null && groupIdBytes > 0) {
                options.put(PusCommandPostprocessor.OPTION_GROUP_ID.getId(), defaultGroupId.doubleValue());
            }
            IssuedCommand cmd;
            try {
                cmd = sctx.uploadEntry(entry.getId(), options);
            } catch (RuntimeException e) {
                sctx.uploadFailed(entry.getId(), e.getMessage());
                return CompletableFuture.failedFuture(e);
            }

            // inner TC (the entry): apid from its binary, seq count filled by the post-processor
            var innerRef = refOf(cmd);
            // outer TC[11,4]
            var outerApid = cmd.attribute("pus11Apid");
            var outerSeq = cmd.attribute("pus11CcsdsSeqCount");
            var outerRef = outerApid.thenCombine(outerSeq,
                    (a, s) -> ref(a.getSint32Value(), s.getSint32Value() & 0x3FFF));

            return innerRef.thenCombine(outerRef, (inner, outer) -> {
                sctx.setUploadRef(entry.getId(), cmd, inner);
                return new String[] { inner, outer };
            }).thenCompose(refs -> {
                var f = new CompletableFuture<Void>();
                synchronized (Pus11SchedulerAdapter.this) {
                    var recent = recentCompletions.remove(refs[1]);
                    if (recent != null) {
                        if (recent.isEmpty()) {
                            f.complete(null);
                        } else {
                            f.completeExceptionally(new IllegalStateException(recent));
                        }
                    } else {
                        pendingInserts.put(refs[1], new PendingInsert(entry.getId(), refs[0]));
                        pendingCompletions.put(refs[1], f);
                    }
                }
                if (f.isDone() && !f.isCompletedExceptionally()) {
                    ctx.reportEvidence(refs[0], ctx.getProcessor().getCurrentTime(), EvidenceSource.REALTIME,
                            EntryState.ONBOARD, "Inserted in the on-board schedule");
                }
                return f;
            }).orTimeout(ackTimeout, TimeUnit.MILLISECONDS).whenComplete((v, err) -> {
                if (err != null) {
                    sctx.uploadFailed(entry.getId(), String.valueOf(unwrap(err).getMessage()));
                }
            });
        }
    }

    private static Throwable unwrap(Throwable t) {
        while (t.getCause() != null && (t instanceof java.util.concurrent.CompletionException)) {
            t = t.getCause();
        }
        if (t instanceof java.util.concurrent.TimeoutException) {
            return new IllegalStateException("no on-board acknowledgement received");
        }
        return t;
    }
}
