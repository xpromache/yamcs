package org.yamcs.pus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yamcs.Processor;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.YConfiguration;
import org.yamcs.obsched.OnboardSnapshot;
import org.yamcs.obsched.ScheduleEntry;
import org.yamcs.obsched.SchedulerContext;
import org.yamcs.protobuf.obsched.EntryIntent;
import org.yamcs.protobuf.obsched.EntryState;
import org.yamcs.protobuf.obsched.RecordSource;
import org.yamcs.protobuf.obsched.ScheduleEntryInfo;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.yarch.Tuple;

public class Pus11SchedulerAdapterTest {

    record Report(String ref, long time, RecordSource source, EntryState state, String message) {
    }

    List<Report> reports = new ArrayList<>();
    Pus11SchedulerAdapter adapter;

    @BeforeAll
    public static void setupTime() {
        TimeEncoding.setUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        adapter = new Pus11SchedulerAdapter();
        var config = adapter.getSpec().validate(YConfiguration.wrap(Map.of()));
        adapter.init(new SchedulerContext() {
            @Override
            public String getYamcsInstance() {
                return "test";
            }

            @Override
            public String getSchedulerName() {
                return "obsched";
            }

            @Override
            public Processor getProcessor() {
                return null;
            }

            @Override
            public ScheduledExecutorService getExecutor() {
                return null;
            }

            @Override
            public void reportStatus(String ref, long time, RecordSource source, EntryState state,
                    String message) {
                reports.add(new Report(ref, time, source, state, message));
            }
        }, config);
    }

    /**
     * TM[1,subtype] about the TC with the given apid/seq, laid out like the PUS simulator (user data at offset 21)
     */
    static byte[] verificationReport(int subtype, int apid, int seq, Integer failureCode) {
        byte[] p = new byte[21 + 4 + (failureCode == null ? 0 : 4)];
        p[0] = 0x08; // secondary header flag
        p[7] = 1;
        p[8] = (byte) subtype;
        p[21] = (byte) (0x18 | (apid >> 8));
        p[22] = (byte) apid;
        p[23] = (byte) (0xC0 | (seq >> 8));
        p[24] = (byte) seq;
        if (failureCode != null) {
            p[28] = (byte) (int) failureCode;
        }
        return p;
    }

    static Tuple tmTuple(long gentime, byte[] packet) {
        var t = new Tuple(StandardTupleDefinitions.TM.copy(), new Object[StandardTupleDefinitions.TM.size()]);
        t.setColumn(StandardTupleDefinitions.GENTIME_COLUMN, gentime);
        t.setColumn(StandardTupleDefinitions.TM_PACKET_COLUMN, packet);
        return t;
    }

    @Test
    public void verificationReportsBecomeStatus() {
        adapter.processPacket(tmTuple(1000, verificationReport(1, 1, 300, null)), RecordSource.DUMP, false);
        adapter.processPacket(tmTuple(1001, verificationReport(3, 1, 300, null)), RecordSource.DUMP, false);
        adapter.processPacket(tmTuple(1002, verificationReport(8, 1, 300, 5)), RecordSource.DUMP, false);

        assertEquals(3, reports.size());
        assertEquals(new Report("1/300", 1000, RecordSource.DUMP, EntryState.RELEASED, "TM[1,1] acceptance"),
                reports.get(0));
        assertEquals(EntryState.STARTED, reports.get(1).state());
        var failure = reports.get(2);
        assertEquals(EntryState.FAILED, failure.state());
        assertTrue(failure.message().contains("code 5"), failure.message());
    }

    @Test
    public void otherPacketsAreIgnored() {
        var p = verificationReport(1, 1, 300, null);
        p[7] = 3; // HK
        adapter.processPacket(tmTuple(1000, p), RecordSource.REALTIME, true);
        assertTrue(reports.isEmpty());
    }

    static ScheduleEntry entry(EntryState state, EntryIntent intent, String ref, long releaseTime, int revision,
            int uploadedRevision) {
        var b = ScheduleEntryInfo.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setScheduler("obsched")
                .setCommand("/PUS17/ARE_YOU_ALIVE")
                .setReleaseTime(TimeEncoding.toProtobufTimestamp(releaseTime))
                .setRevision(revision)
                .setUploadedRevision(uploadedRevision)
                .setIntent(intent)
                .setState(state);
        if (ref != null) {
            b.setRef(ref);
            b.addUploads(org.yamcs.protobuf.obsched.UploadInfo.newBuilder().setCommandId("1-x-1").setRef(ref));
        }
        return new ScheduleEntry(b.build());
    }

    @Test
    public void planSync() {
        long now = 1_000_000;
        var toInsert = entry(EntryState.NOT_ONBOARD, EntryIntent.PLANNED, null, now + 60_000, 1, 0);
        var tooLate = entry(EntryState.NOT_ONBOARD, EntryIntent.PLANNED, null, now + 1_000, 1, 0);
        var toDelete = entry(EntryState.ONBOARD, EntryIntent.DELETE_PENDING, "1/10", now + 60_000, 1, 1);
        var toReplace = entry(EntryState.ONBOARD, EntryIntent.PLANNED, "1/11", now + 60_000, 2, 1);
        var unchanged = entry(EntryState.ONBOARD, EntryIntent.PLANNED, "1/12", now + 60_000, 1, 1);

        var snapshot = new OnboardSnapshot(now, List.of(
                new OnboardSnapshot.OnboardItem("1/10", now + 60_000, null),
                new OnboardSnapshot.OnboardItem("1/11", now + 60_000, null),
                new OnboardSnapshot.OnboardItem("1/12", now + 60_000, null)));

        var plan = adapter.planSync(List.of(toInsert, tooLate, toDelete, toReplace, unchanged), snapshot,
                now + 10_000);
        var steps = plan.getSteps();
        assertEquals(3, steps.size(), steps.stream().map(s -> s.getDescription()).toList().toString());
        // one TC[11,5] for the deletion and the old copy of the replaced entry
        assertTrue(steps.get(0).getDescription().startsWith("TC[11,5] delete 2 activities"));
        assertEquals(List.of(toDelete.getId(), toReplace.getId()), steps.get(0).getEntryIds());
        // then the inserts
        assertEquals(List.of(toInsert.getId()), steps.get(1).getEntryIds());
        assertEquals(List.of(toReplace.getId()), steps.get(2).getEntryIds());
    }
}
