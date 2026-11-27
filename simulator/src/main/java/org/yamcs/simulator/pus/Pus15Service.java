package org.yamcs.simulator.pus;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Minimal ST[15] on-board storage and retrieval (ECSS-E-ST-70-41C chapter 6.15).
 * <p>
 * There is a single circular packet store with the ID {@link #PACKET_STORE_ID}. When the storage function is enabled
 * (the default), every TM packet that the simulator would have transmitted in realtime during an LOS is stored. During
 * AOS the packets go out in realtime and are <b>not</b> stored. The ST[9] time packets (APID 0) are not stored either.
 * <p>
 * The stored packets are downlinked on the dump (LOS) link, never on the realtime link, either following a TC[15,9]
 * or automatically right after AOS (see {@link PusSimulator#setAutoDumpOnAos(boolean)}).
 * <p>
 * Supported requests (all the packet store IDs are 1 byte, N is 1 byte, the times are absolute CUC times encoded like
 * all the other PUS times of the simulator: 1 byte pfield 0x2F, 4 bytes seconds since the Unix epoch, 3 bytes
 * fraction):
 *
 * <pre>
 * TC[15,1]  enable the storage function:       N, N x packet store ID   (N = 0 means all packet stores)
 * TC[15,2]  disable the storage function:      N, N x packet store ID   (N = 0 means all packet stores)
 * TC[15,9]  start the by-time-range retrieval: packet store ID, start time, end time
 *           The packets with a storage time in [start, end] are downlinked in storage order on the dump link.
 *           The completion report is generated when all of them have been handed to the dump link.
 * TC[15,11] delete the packet store content up to the specified time: end time, N, N x packet store ID
 *           (N = 0 means all packet stores). Deletes the packets with storage time &lt;= end time.
 * TC[15,12] summary-report the packet store content: N, N x packet store ID (N = 0 means all packet stores)
 *
 * TM[15,13] packet store content summary report: N, then N x (
 *              packet store ID (1 byte),
 *              oldest stored packet time (8 bytes, 0 if the store is empty),
 *              newest stored packet time (8 bytes, 0 if the store is empty),
 *              percentage filled (1 byte),
 *              number of stored packets (4 bytes, not in the standard))
 * </pre>
 */
public class Pus15Service extends AbstractPusService {
    public static final int PACKET_STORE_ID = 1;
    public static final int DEFAULT_CAPACITY = 100_000;

    // ST[15] start errors
    static final int START_ERR_UNKNOWN_PACKET_STORE = 14;
    static final int START_ERR_NO_DOWNLINK = 15;
    // ST[15] completion errors
    static final int COMPL_ERR_DUMP_ABORTED_BY_LOS = 6;

    static final PusTime ZERO_TIME = new PusTime(0, 0);

    final PacketStore store;
    volatile boolean storageEnabled = true;
    // store sequence of the last packet dumped by the automatic dump at AOS
    long lastAutoDumpedSeq = 0;

    final ExecutorService dumpExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "pus15-dump");
        t.setDaemon(true);
        return t;
    });

    Pus15Service(PusSimulator pusSimulator) {
        this(pusSimulator, DEFAULT_CAPACITY);
    }

    Pus15Service(PusSimulator pusSimulator, int capacity) {
        super(pusSimulator, 15);
        store = new PacketStore(PACKET_STORE_ID, capacity);
    }

    /**
     * Called by the simulator for each TM packet which could not be transmitted because of LOS.
     */
    void storeLosPacket(byte[] packet) {
        if (storageEnabled) {
            store.add(PusTime.now(), packet);
        }
    }

    /**
     * Downlink on the dump link all the packets stored since the last automatic dump.
     */
    void autoDump() {
        dumpExecutor.execute(() -> {
            List<StoredPacket> pkts;
            synchronized (this) {
                pkts = store.since(lastAutoDumpedSeq);
            }
            if (pkts.isEmpty()) {
                return;
            }
            if (!pusSimulator.isDumpLinkAvailable()) {
                log.warn("Dump link not available, not dumping the {} packets stored since the last dump",
                        pkts.size());
                return;
            }
            log.info("AOS: dumping {} packets stored since the last dump", pkts.size());
            long last = dump(pkts);
            synchronized (this) {
                lastAutoDumpedSeq = Math.max(lastAutoDumpedSeq, last);
            }
        });
    }

    /**
     * sends the packets on the dump link, stops at LOS
     *
     * @return the sequence of the last sent packet (or -1 if none has been sent)
     */
    private long dump(List<StoredPacket> pkts) {
        long last = -1;
        for (StoredPacket p : pkts) {
            if (pusSimulator.isLOS()) {
                log.warn("LOS during the dump, aborting it");
                break;
            }
            pusSimulator.transmitDump(p.data);
            last = p.seq;
        }
        return last;
    }

    @Override
    public void executeTc(PusTcPacket tc) {
        try {
            switch (tc.getSubtype()) {
            case 1 -> setStorageFunction(tc, true);
            case 2 -> setStorageFunction(tc, false);
            case 9 -> startByTimeRangeRetrieval(tc);
            case 11 -> deleteUpTo(tc);
            case 12 -> summaryReport(tc);
            default -> {
                log.info("invalid subtype {}, sending NACK start", tc.getSubtype());
                nack_start(tc, START_ERR_INVALID_PUS_SUBTYPE);
            }
            }
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            log.warn("Invalid TC[15,{}]: {}", tc.getSubtype(), e.toString());
            nack_start(tc, START_ERR_INVALID_REQUEST);
        }
    }

    /**
     * reads N followed by N packet store ids, returns false (and sends a NACK start) if any of them is unknown
     */
    private boolean checkStoreIds(PusTcPacket tc, ByteBuffer bb) {
        int n = bb.get() & 0xFF;
        for (int i = 0; i < n; i++) {
            int id = bb.get() & 0xFF;
            if (id != PACKET_STORE_ID) {
                log.info("TC[15,{}]: unknown packet store {}", tc.getSubtype(), id);
                nack_start(tc, START_ERR_UNKNOWN_PACKET_STORE);
                return false;
            }
        }
        return true;
    }

    private void setStorageFunction(PusTcPacket tc, boolean enabled) {
        if (!checkStoreIds(tc, tc.getUserDataBuffer())) {
            return;
        }
        ack_start(tc);
        storageEnabled = enabled;
        log.info("Storage function of packet store {} {}", PACKET_STORE_ID, enabled ? "enabled" : "disabled");
        ack_completion(tc);
    }

    private void startByTimeRangeRetrieval(PusTcPacket tc) {
        ByteBuffer bb = tc.getUserDataBuffer();
        int id = bb.get() & 0xFF;
        PusTime start = PusTime.read(bb);
        PusTime end = PusTime.read(bb);
        if (id != PACKET_STORE_ID) {
            nack_start(tc, START_ERR_UNKNOWN_PACKET_STORE);
            return;
        }
        if (!pusSimulator.isDumpLinkAvailable()) {
            log.info("TC[15,9]: the dump link is not available");
            nack_start(tc, START_ERR_NO_DOWNLINK);
            return;
        }
        ack_start(tc);
        dumpExecutor.execute(() -> {
            List<StoredPacket> pkts;
            synchronized (this) {
                pkts = store.inRange(start, end);
            }
            log.info("TC[15,9]: dumping {} packets stored between {} and {}", pkts.size(), start, end);
            int sent = 0;
            for (StoredPacket p : pkts) {
                if (pusSimulator.isLOS()) {
                    break;
                }
                pusSimulator.transmitDump(p.data);
                sent++;
            }
            if (sent == pkts.size()) {
                ack_completion(tc);
            } else {
                log.warn("LOS during the TC[15,9] retrieval, {} out of {} packets dumped", sent, pkts.size());
                nack_completion(tc, COMPL_ERR_DUMP_ABORTED_BY_LOS);
            }
        });
    }

    private void deleteUpTo(PusTcPacket tc) {
        ByteBuffer bb = tc.getUserDataBuffer();
        PusTime end = PusTime.read(bb);
        if (!checkStoreIds(tc, bb)) {
            return;
        }
        ack_start(tc);
        int n;
        synchronized (this) {
            n = store.deleteUpTo(end);
        }
        log.info("TC[15,11]: deleted {} packets stored up to {}", n, end);
        ack_completion(tc);
    }

    private void summaryReport(PusTcPacket tc) {
        if (!checkStoreIds(tc, tc.getUserDataBuffer())) {
            return;
        }
        ack_start(tc);
        PusTmPacket pkt = newPacket(13, 1 + 1 + 2 * PusTime.LENGTH_BYTES + 1 + 4);
        ByteBuffer bb = pkt.getUserDataBuffer();
        synchronized (this) {
            bb.put((byte) 1);
            bb.put((byte) PACKET_STORE_ID);
            store.oldestTime().encode(bb);
            store.newestTime().encode(bb);
            bb.put((byte) store.fillPercentage());
            bb.putInt(store.size());
        }
        pusSimulator.transmitRealtimeTM(pkt);
        ack_completion(tc);
    }

    synchronized int storedCount() {
        return store.size();
    }

    synchronized int fillPercentage() {
        return store.fillPercentage();
    }

    void stop() {
        dumpExecutor.shutdownNow();
    }

    record StoredPacket(long seq, PusTime time, byte[] data) {
    }

    /**
     * Circular packet store: when full, the oldest packet is overwritten.
     * <p>
     * Not thread safe, the access is synchronized by the service.
     */
    static class PacketStore {
        final int id;
        final int capacity;
        final Deque<StoredPacket> packets = new ArrayDeque<>();
        long seq = 0;

        PacketStore(int id, int capacity) {
            this.id = id;
            this.capacity = capacity;
        }

        synchronized void add(PusTime time, byte[] data) {
            if (packets.size() >= capacity) {
                packets.removeFirst();
            }
            packets.addLast(new StoredPacket(++seq, time, data));
        }

        synchronized List<StoredPacket> since(long lastSeq) {
            List<StoredPacket> l = new ArrayList<>();
            for (StoredPacket p : packets) {
                if (p.seq > lastSeq) {
                    l.add(p);
                }
            }
            return l;
        }

        synchronized List<StoredPacket> inRange(PusTime start, PusTime end) {
            List<StoredPacket> l = new ArrayList<>();
            for (StoredPacket p : packets) {
                if (!p.time.isBefore(start) && !p.time.isAfter(end)) {
                    l.add(p);
                }
            }
            return l;
        }

        synchronized int deleteUpTo(PusTime end) {
            int n = 0;
            while (!packets.isEmpty() && !packets.peekFirst().time.isAfter(end)) {
                packets.removeFirst();
                n++;
            }
            return n;
        }

        synchronized PusTime oldestTime() {
            return packets.isEmpty() ? ZERO_TIME : packets.peekFirst().time;
        }

        synchronized PusTime newestTime() {
            return packets.isEmpty() ? ZERO_TIME : packets.peekLast().time;
        }

        synchronized int size() {
            return packets.size();
        }

        synchronized int fillPercentage() {
            return (int) (100L * packets.size() / capacity);
        }
    }
}
