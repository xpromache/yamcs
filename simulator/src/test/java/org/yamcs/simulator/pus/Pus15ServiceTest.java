package org.yamcs.simulator.pus;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yamcs.utils.ByteArrayUtils;

public class Pus15ServiceTest {
    static final int DATA_OFFSET = PusTmPacket.DATA_OFFSET;

    TestSim sim;

    @BeforeAll
    static void setupTime() {
        org.yamcs.utils.TimeEncoding.setUp();
    }

    @BeforeEach
    void setup() throws Exception {
        File dir = Files.createTempDirectory("pus15test").toFile();
        dir.deleteOnExit();
        sim = new TestSim(dir);
    }

    @AfterEach
    void teardown() {
        if (sim.started) {
            sim.doStop();
        } else {
            sim.executor.shutdownNow();
            sim.pus15Service.stop();
        }
    }

    @Test
    void storesOnlyDuringLos() {
        sim.setAutoDumpOnAos(false);
        sim.transmitRealtimeTM(tm17());
        assertEquals(1, sim.realtime.size());
        assertEquals(0, sim.pus15Service.storedCount());

        sim.setLOS();
        sim.transmitRealtimeTM(tm17());
        sim.transmitRealtimeTM(tm17());
        assertEquals(1, sim.realtime.size());
        assertEquals(2, sim.pus15Service.storedCount());

        sim.setAOS();
        sim.transmitRealtimeTM(tm17());
        assertEquals(2, sim.realtime.size());
        assertEquals(2, sim.pus15Service.storedCount());
        assertTrue(sim.dump.isEmpty());
    }

    @Test
    void storageDisabledDoesNotStore() {
        sim.pus15Service.executeTc(storeIdsTc(2, PACKET_STORE));
        assertNotNull(lastReport(sim.realtime, 1, 7));

        sim.setLOS();
        sim.transmitRealtimeTM(tm17());
        assertEquals(0, sim.pus15Service.storedCount());
        sim.setAOS();

        sim.pus15Service.executeTc(storeIdsTc(1, PACKET_STORE));
        sim.setLOS();
        sim.transmitRealtimeTM(tm17());
        assertEquals(1, sim.pus15Service.storedCount());
    }

    @Test
    void unknownPacketStoreRejected() {
        sim.pus15Service.executeTc(storeIdsTc(12, new int[] { 7 }));
        byte[] nack = lastReport(sim.realtime, 1, 4);
        assertNotNull(nack);
        assertEquals(Pus15Service.START_ERR_UNKNOWN_PACKET_STORE, ByteArrayUtils.decodeInt(nack, DATA_OFFSET + 4));
    }

    @Test
    void autoDumpOnAosDumpsOnlyWhatWasStoredSinceLastDump() throws Exception {
        sim.setLOS();
        List<byte[]> los1 = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            los1.add(transmit(tm17()));
        }
        sim.setAOS();
        waitFor(() -> sim.dump.size() == 3);
        for (int i = 0; i < 3; i++) {
            assertArrayEquals(los1.get(i), sim.dump.get(i));
        }

        sim.setLOS();
        byte[] p4 = transmit(tm17());
        byte[] p5 = transmit(tm17());
        sim.setAOS();
        waitFor(() -> sim.dump.size() == 5);
        Thread.sleep(200);
        assertEquals(5, sim.dump.size());
        assertArrayEquals(p4, sim.dump.get(3));
        assertArrayEquals(p5, sim.dump.get(4));
        // the store keeps the packets until they are deleted
        assertEquals(5, sim.pus15Service.storedCount());
    }

    @Test
    void byTimeRangeRetrieval() throws Exception {
        sim.setAutoDumpOnAos(false);
        sim.setLOS();
        byte[] p1 = transmit(tm17());
        Thread.sleep(50);
        PusTime start = PusTime.now();
        byte[] p2 = transmit(tm17());
        byte[] p3 = transmit(tm17());
        PusTime end = PusTime.now();
        Thread.sleep(50);
        transmit(tm17());
        sim.setAOS();
        Thread.sleep(100);
        assertTrue(sim.dump.isEmpty());

        // retrieval during LOS is rejected
        sim.setLOS();
        sim.pus15Service.executeTc(retrievalTc(PACKET_STORE_ID, start, end));
        sim.setAOS();
        assertTrue(sim.dump.isEmpty());

        sim.pus15Service.executeTc(retrievalTc(PACKET_STORE_ID, start, end));
        waitFor(() -> lastReport(sim.realtime, 1, 7) != null);
        assertEquals(2, sim.dump.size());
        assertArrayEquals(p2, sim.dump.get(0));
        assertArrayEquals(p3, sim.dump.get(1));
        assertTrue(sim.dump.stream().noneMatch(p -> java.util.Arrays.equals(p, p1)));
    }

    @Test
    void deleteUpToAndSummaryReport() throws Exception {
        sim.setAutoDumpOnAos(false);
        sim.setLOS();
        transmit(tm17());
        transmit(tm17());
        Thread.sleep(50);
        PusTime t = PusTime.now();
        Thread.sleep(50);
        transmit(tm17());
        sim.setAOS();

        sim.pus15Service.executeTc(storeIdsTc(12, PACKET_STORE));
        byte[] report = lastReport(sim.realtime, 15, 13);
        assertNotNull(report);
        ByteBuffer bb = ByteBuffer.wrap(report, DATA_OFFSET, report.length - DATA_OFFSET);
        assertEquals(1, bb.get());
        assertEquals(PACKET_STORE_ID, bb.get());
        PusTime oldest = PusTime.read(bb);
        PusTime newest = PusTime.read(bb);
        assertTrue(oldest.isBefore(t));
        assertTrue(newest.isAfter(t));
        assertEquals(0, bb.get()); // fill percentage
        assertEquals(3, bb.getInt());

        sim.pus15Service.executeTc(deleteTc(t, PACKET_STORE));
        assertEquals(1, sim.pus15Service.storedCount());

        sim.pus15Service.executeTc(deleteTc(PusTime.now(), new int[0])); // N=0 means all stores
        assertEquals(0, sim.pus15Service.storedCount());
    }

    @Test
    void uplinkTcDroppedDuringLos() throws Exception {
        sim.startSim();
        sim.setLOS();
        PusTcPacket tc = new PusTcPacket(1, 0, 0, 17, 1);
        tc.fillChecksum();
        sim.processUplinkTc(tc);
        Thread.sleep(500);
        sim.setAOS();
        Thread.sleep(300);
        assertTrue(findReports(sim.allTm(), 1, 1, 1, tc.getSequenceCount()).isEmpty());
    }

    /**
     * Schedule a TC[17,1] via ST[11], enter LOS, let it release, return to AOS and check that its PUS 1 reports come
     * out on the dump link.
     */
    @Test
    void scheduledCommandReleasedDuringLosIsReportedInTheDump() throws Exception {
        sim.startSim();

        sim.processUplinkTc(tc11(22, new byte[] { 1, 2, 1 })); // create group 2 enabled
        PusTcPacket embedded = new PusTcPacket(1, 0, 0, 17, 1);
        sim.processUplinkTc(insertTc(1, 2, PusTime.now().shiftByMillis(1500), embedded));
        sim.processUplinkTc(tc11(20, new byte[] { 1, 1 })); // enable sub-schedule 1
        waitFor(() -> findReports(sim.realtime, 1, 7, -1, -1).size() >= 3);

        sim.setLOS();
        int rtCount = sim.realtime.size();

        Thread.sleep(2500); // the command is released during LOS
        assertEquals(rtCount, sim.realtime.size(), "no realtime TM during LOS");
        assertTrue(sim.pus15Service.storedCount() > 0);

        sim.setAOS();
        int apid = embedded.getAPID();
        int seq = embedded.getSequenceCount();
        waitFor(() -> findReports(sim.dump, 1, 7, apid, seq).size() == 1);

        List<byte[]> acc = findReports(sim.dump, 1, 1, apid, seq);
        List<byte[]> compl = findReports(sim.dump, 1, 7, apid, seq);
        assertEquals(1, acc.size());
        assertEquals(1, compl.size());
        assertTrue(indexOf(sim.dump, acc.get(0)) < indexOf(sim.dump, compl.get(0)));
        // the TM[17,2] response is dumped as well
        assertTrue(sim.dump.stream().anyMatch(p -> u8(p, 7) == 17 && u8(p, 8) == 2));
        // and none of the reports went out in realtime
        assertTrue(findReports(sim.realtime, 1, 1, apid, seq).isEmpty());
        assertTrue(findReports(sim.realtime, 1, 7, apid, seq).isEmpty());
    }

    // ---- helpers ----
    static final int PACKET_STORE_ID = Pus15Service.PACKET_STORE_ID;
    static final int[] PACKET_STORE = { PACKET_STORE_ID };

    byte[] transmit(PusTmPacket p) {
        sim.transmitRealtimeTM(p);
        return p.getBytes().clone();
    }

    static PusTmPacket tm17() {
        return new PusTmPacket(1, 0, 17, 2);
    }

    static PusTcPacket tc(int type, int subtype, byte[] data) {
        PusTcPacket p = new PusTcPacket(1, data.length, 0, type, subtype);
        p.getUserDataBuffer().put(data);
        p.fillChecksum();
        return p;
    }

    static PusTcPacket tc11(int subtype, byte[] data) {
        return tc(11, subtype, data);
    }

    static PusTcPacket storeIdsTc(int subtype, int[] ids) {
        ByteBuffer bb = ByteBuffer.allocate(1 + ids.length);
        bb.put((byte) ids.length);
        for (int id : ids) {
            bb.put((byte) id);
        }
        return tc(15, subtype, bb.array());
    }

    static PusTcPacket retrievalTc(int id, PusTime start, PusTime end) {
        ByteBuffer bb = ByteBuffer.allocate(1 + 2 * PusTime.LENGTH_BYTES);
        bb.put((byte) id);
        start.encode(bb);
        end.encode(bb);
        return tc(15, 9, bb.array());
    }

    static PusTcPacket deleteTc(PusTime t, int[] ids) {
        ByteBuffer bb = ByteBuffer.allocate(PusTime.LENGTH_BYTES + 1 + ids.length);
        t.encode(bb);
        bb.put((byte) ids.length);
        for (int id : ids) {
            bb.put((byte) id);
        }
        return tc(15, 11, bb.array());
    }

    static PusTcPacket insertTc(int subschedule, int group, PusTime release, PusTcPacket embeddedTc) {
        embeddedTc.fillChecksum();
        byte[] embedded = embeddedTc.getBytes();
        ByteBuffer ud = ByteBuffer.allocate(3 + PusTime.LENGTH_BYTES + embedded.length);
        ud.put((byte) subschedule);
        ud.put((byte) 1); // N
        ud.put((byte) group);
        release.encode(ud);
        ud.put(embedded);
        return tc11(4, ud.array());
    }

    static int u8(byte[] b, int i) {
        return b[i] & 0xff;
    }

    static int indexOf(List<byte[]> l, byte[] p) {
        for (int i = 0; i < l.size(); i++) {
            if (l.get(i) == p) {
                return i;
            }
        }
        return -1;
    }

    static byte[] lastReport(List<byte[]> tm, int type, int subtype) {
        byte[] found = null;
        for (byte[] p : tm) {
            if (p.length > DATA_OFFSET && u8(p, 7) == type && u8(p, 8) == subtype) {
                found = p;
            }
        }
        return found;
    }

    /**
     * finds the PUS1 reports of the given subtype for the TC with the given apid and seq count (-1 = any)
     */
    static List<byte[]> findReports(List<byte[]> tm, int type, int subtype, int apid, int seq) {
        List<byte[]> l = new ArrayList<>();
        for (byte[] p : tm) {
            if (p.length < DATA_OFFSET + 4 || u8(p, 7) != type || u8(p, 8) != subtype) {
                continue;
            }
            int tcApid = ByteArrayUtils.decodeUnsignedShort(p, DATA_OFFSET) & 0x7FF;
            int tcSeq = ByteArrayUtils.decodeUnsignedShort(p, DATA_OFFSET + 2) & 0x3FFF;
            if ((apid < 0 || apid == tcApid) && (seq < 0 || seq == tcSeq)) {
                l.add(p);
            }
        }
        return l;
    }

    static void waitFor(BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("condition not met in time");
            }
            Thread.sleep(20);
        }
    }

    static class TestSim extends PusSimulator {
        final List<byte[]> realtime = new CopyOnWriteArrayList<>();
        final List<byte[]> dump = new CopyOnWriteArrayList<>();
        boolean started;

        TestSim(File dir) {
            super(dir);
            executor = new ScheduledThreadPoolExecutor(1);
        }

        @Override
        protected void sendRealtime(byte[] packet) {
            realtime.add(packet.clone());
        }

        @Override
        protected void transmitDump(byte[] packet) {
            dump.add(packet.clone());
        }

        @Override
        protected boolean isDumpLinkAvailable() {
            return !isLOS();
        }

        void startSim() {
            executor.shutdownNow(); // doStart creates its own
            doStart();
            started = true;
        }

        List<byte[]> allTm() {
            List<byte[]> l = new ArrayList<>(realtime);
            l.addAll(dump);
            return l;
        }
    }
}
