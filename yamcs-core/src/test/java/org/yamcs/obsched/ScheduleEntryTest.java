package org.yamcs.obsched;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yamcs.protobuf.obsched.EntryState;
import org.yamcs.protobuf.obsched.RecordSource;
import org.yamcs.utils.TimeEncoding;

public class ScheduleEntryTest {

    @BeforeAll
    public static void setup() {
        TimeEncoding.setUp();
    }

    private ScheduleEntry newEntry() {
        var e = new ScheduleEntry(UUID.randomUUID(), "test");
        e.setCommand("/PUS17/ARE_YOU_ALIVE");
        e.setReleaseTime(1000_000);
        return e;
    }

    @Test
    public void statusOnlyMovesForward() {
        var e = newEntry();
        e.forceState(1, RecordSource.GROUND, EntryState.ONBOARD, null);

        // completion arrives before the release report (out of order dump)
        assertTrue(e.applyStatus(2, 10, RecordSource.DUMP, EntryState.COMPLETED, "TM[1,7]"));
        assertFalse(e.applyStatus(1, 10, RecordSource.DUMP, EntryState.RELEASED, "TM[1,1]"));
        assertEquals(EntryState.COMPLETED, e.getState());
        // all the reports are recorded
        assertEquals(3, e.getHistory().size());
        assertTrue(e.isReleased());
    }

    @Test
    public void unknownIsUpgradedByLateReport() {
        var e = newEntry();
        e.forceState(1, RecordSource.READBACK, EntryState.UNKNOWN, "no report");
        assertTrue(e.applyStatus(2, 10, RecordSource.DUMP, EntryState.RELEASED, "TM[1,1]"));
        assertEquals(EntryState.RELEASED, e.getState());
    }

    @Test
    public void lateOnboardDoesNotHideUnknown() {
        var e = newEntry();
        e.forceState(1, RecordSource.READBACK, EntryState.UNKNOWN, "no report");
        assertFalse(e.applyStatus(2, 10, RecordSource.REALTIME, EntryState.UPLOAD_PENDING, null));
        assertEquals(EntryState.UNKNOWN, e.getState());
    }

    @Test
    public void refsOfAllUploadsAreKept() {
        var e = newEntry();
        e.addUpload("1-a-1", 1);
        e.setUploadRef("1-a-1", "1/4");
        e.incrementRevision();
        e.addUpload("2-a-1", 2);
        e.setUploadRef("2-a-1", "1/9");

        assertEquals("1/9", e.getRef());
        assertTrue(e.hasRef("1/4"));
        assertTrue(e.hasRef("1/9"));
        assertEquals("1-a-1", e.getUploadCommandId("1/4"));
        assertFalse(e.hasRef("1/5"));
    }

    @Test
    public void protoRoundTrip() {
        var e = newEntry();
        e.setDescription("ping");
        e.addUpload("1-a-1", 1);
        e.setUploadRef("1-a-1", "1/4");
        e.forceState(5, RecordSource.GROUND, EntryState.ONBOARD, "inserted");
        var copy = new ScheduleEntry(e.toProto(true));
        assertEquals(e.getId(), copy.getId());
        assertEquals(e.getReleaseTime(), copy.getReleaseTime());
        assertEquals(EntryState.ONBOARD, copy.getState());
        assertEquals("1/4", copy.getRef());
        assertEquals(1, copy.getHistory().size());
        assertEquals("ping", copy.getDescription());
    }

    static final long TOLERANCE = 5_000;
    static final long WINDOW = 60_000;

    @Test
    public void reportsAboutAReusedRefAreNotPlausible() {
        // uploaded at 900 s, released at 1000 s
        var e = newEntry();
        e.addUpload("1-a-1", 900_000);
        e.setUploadRef("1-a-1", "1/4");
        e.forceState(900_000, RecordSource.GROUND, EntryState.ONBOARD, "inserted");

        // its own release and completion
        assertTrue(e.isPlausibleReport("1/4", 1000_000, EntryState.RELEASED, TOLERANCE, WINDOW));
        assertTrue(e.isPlausibleReport("1/4", 998_000, EntryState.RELEASED, TOLERANCE, WINDOW)); // clock difference
        assertTrue(e.isPlausibleReport("1/4", 1050_000, EntryState.COMPLETED, TOLERANCE, WINDOW));

        // another command with the same apid/seq, sent directly before the release
        assertFalse(e.isPlausibleReport("1/4", 950_000, EntryState.RELEASED, TOLERANCE, WINDOW));
        assertFalse(e.isPlausibleReport("1/4", 950_000, EntryState.COMPLETED, TOLERANCE, WINDOW));
        // or before the upload
        assertFalse(e.isPlausibleReport("1/4", 800_000, EntryState.COMPLETED, TOLERANCE, WINDOW));

        // the insert confirmation is not about the execution
        assertTrue(e.isPlausibleReport("1/4", 900_500, EntryState.ONBOARD, TOLERANCE, WINDOW));
    }

    @Test
    public void lateReportsOnlyWhileExecuting() {
        var e = newEntry();
        e.addUpload("1-a-1", 900_000);
        e.setUploadRef("1-a-1", "1/4");
        e.forceState(900_000, RecordSource.GROUND, EntryState.ONBOARD, "inserted");
        e.applyStatus(1000_000, 1000_000, RecordSource.REALTIME, EntryState.STARTED, "TM[1,3]");

        // a long running command completing 10 minutes after its release
        assertTrue(e.isPlausibleReport("1/4", 1600_000, EntryState.COMPLETED, TOLERANCE, WINDOW));
        // but not a new release
        assertFalse(e.isPlausibleReport("1/4", 1600_000, EntryState.RELEASED, TOLERANCE, WINDOW));

        // once completed, a later run with the same ref is someone else
        e.applyStatus(1001_000, 1001_000, RecordSource.REALTIME, EntryState.COMPLETED, "TM[1,7]");
        assertFalse(e.isPlausibleReport("1/4", 1600_000, EntryState.STARTED, TOLERANCE, WINDOW));
        assertFalse(e.isPlausibleReport("1/4", 1600_000, EntryState.COMPLETED, TOLERANCE, WINDOW));
        // a dump repeating its own reports still fits
        assertTrue(e.isPlausibleReport("1/4", 1001_000, EntryState.COMPLETED, TOLERANCE, WINDOW));
    }
}
