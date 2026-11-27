package org.yamcs.obsched;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yamcs.protobuf.obsched.EntryState;
import org.yamcs.protobuf.obsched.EvidenceSource;
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
    public void evidenceOnlyMovesForward() {
        var e = newEntry();
        e.forceState(1, EvidenceSource.GROUND, EntryState.ONBOARD, null);

        // completion arrives before the release report (out of order dump)
        assertTrue(e.applyEvidence(2, 10, EvidenceSource.DUMP, EntryState.COMPLETED, "TM[1,7]"));
        assertFalse(e.applyEvidence(1, 10, EvidenceSource.DUMP, EntryState.RELEASED, "TM[1,1]"));
        assertEquals(EntryState.COMPLETED, e.getState());
        // all the evidence is recorded
        assertEquals(3, e.getEvidence().size());
        assertTrue(e.isReleased());
    }

    @Test
    public void unknownIsUpgradedByLateEvidence() {
        var e = newEntry();
        e.forceState(1, EvidenceSource.READBACK, EntryState.UNKNOWN, "no evidence");
        assertTrue(e.applyEvidence(2, 10, EvidenceSource.DUMP, EntryState.RELEASED, "TM[1,1]"));
        assertEquals(EntryState.RELEASED, e.getState());
    }

    @Test
    public void lateOnboardDoesNotHideUnknown() {
        var e = newEntry();
        e.forceState(1, EvidenceSource.READBACK, EntryState.UNKNOWN, "no evidence");
        assertFalse(e.applyEvidence(2, 10, EvidenceSource.REALTIME, EntryState.UPLOAD_PENDING, null));
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
        e.forceState(5, EvidenceSource.GROUND, EntryState.ONBOARD, "inserted");
        var copy = new ScheduleEntry(e.toProto(true));
        assertEquals(e.getId(), copy.getId());
        assertEquals(e.getReleaseTime(), copy.getReleaseTime());
        assertEquals(EntryState.ONBOARD, copy.getState());
        assertEquals("1/4", copy.getRef());
        assertEquals(1, copy.getEvidence().size());
        assertEquals("ping", copy.getDescription());
    }
}
