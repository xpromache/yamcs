package org.yamcs.simulator.pus;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.cfdp.pdu.CfdpPacket;
import org.yamcs.simulator.AbstractSimulator;
import org.yamcs.simulator.cfdp.CfdpCcsdsPacket;
import org.yamcs.simulator.cfdp.CfdpReceiver;
import org.yamcs.simulator.cfdp.CfdpSender;
import org.yamcs.simulator.DHSHandler;
import org.yamcs.simulator.EpsLvpduHandler;
import org.yamcs.simulator.FlightDataHandler;
import org.yamcs.simulator.PowerHandler;
import org.yamcs.simulator.RCSHandler;
import org.yamcs.simulator.SimulatorCcsdsPacket;
import org.yamcs.simulator.TcpTmTcLink;
import org.yamcs.simulator.UdpTmFrameLink;

/**
 * PUS (Packet Utilisation Standard) simulator.
 * 
 * Supports services:
 * <ul>
 * <li>ST[01] - request verification</li>
 * <li>ST[03] - housekeeping</li>
 * <li>ST[05] - event reporting - TODO</li>
 * <li>ST[06] - memory management - TODO</li>
 * <li>ST[09] - time management - only sending the time packet</li>
 * <li>ST[11] - time based schedule</li>
 * <li>ST[12] - on-board monitoring - TODO</li>
 * <li>ST[13] - large packet transfer - TODO</li>
 * <li>ST[15] - on-board storage and retrieval - one packet store filled during LOS, see {@link Pus15Service}</li>
 * <li>ST[17] - test</li>
 * <li>ST[20] - on-board parameter management (not the parameter definitions)</li>
 * <li>ST[22] - position based schedule</li>
 * <li>ST[23] - file management - TODO</li>
 * 
 * <li>
 * 
 * </ul>
 * <p>
 * <b>LOS/AOS</b>: the simulator starts in AOS. It can be put in LOS/AOS with {@link #setLOS()}/{@link #setAOS()}
 * (telnet commands {@code LOS}/{@code AOS} or {@code :LOS:START}/{@code :LOS:STOP}) or automatically with
 * {@link #setLosCycle(int, int)}. During LOS:
 * <ul>
 * <li>no TM is transmitted on the realtime link or in frames. The packets are stored instead in the ST[15] packet
 * store (if the storage function is enabled), including the verification reports of the TCs released on-board.</li>
 * <li>the TCs received from the ground are dropped (no uplink); the TCs released by the on-board schedules are still
 * executed.</li>
 * <li>the ST[9] time packets are not sent and not stored.</li>
 * </ul>
 * After AOS, if {@link #setAutoDumpOnAos(boolean) autoDumpOnAos} is true (default), the packets stored since the last
 * automatic dump are downlinked on the dump (LOS) link.
 * <p>
 * The housekeeping packet with hkid 6 contains the on-board view of the link and storage state: LOS flag (1 byte),
 * storage function enabled (1 byte), number of stored packets (4 bytes), packet store fill percentage (1 byte).
 */
public class PusSimulator extends AbstractSimulator {
    static final int MAIN_APID = 1;

    static final int PUS_TYPE_ACK = 1;
    static final int PUS_TYPE_HK = 3;
    static final int PUS_TYPE_EVENT = 5;

    static final int PUS_SUBTYPE_ACK_ACCEPTANCE = 1;
    static final int PUS_SUBTYPE_NACK_ACCEPTANCE = 2;
    static final int PUS_SUBTYPE_ACK_START = 3;
    static final int PUS_SUBTYPE_NACK_START = 4;
    static final int PUS_SUBTYPE_ACK_COMPLETION = 7;
    static final int PUS_SUBTYPE_NACK_COMPLETION = 8;

    static final int START_FAILURE_INVALID_VOLTAGE_NUM = 100;
    private static final Logger log = LoggerFactory.getLogger(PusSimulator.class);

    final Random random = new Random();

    ScheduledThreadPoolExecutor executor;
    TcpTmTcLink tmLink;
    TcpTmTcLink losLink;
    UdpTmFrameLink tmFrameLink;

    FlightDataHandler flightDataHandler;
    DHSHandler dhsHandler;
    PowerHandler powerDataHandler;
    RCSHandler rcsHandler;
    EpsLvpduHandler epslvpduHandler;
    CfdpReceiver cfdpReceiver;
    private volatile CfdpSender cfdpSender;
    Pus5Service pus5Service;
    Pus11Service pus11Service;
    Pus15Service pus15Service;
    Pus17Service pus17Service;
    Pus19Service pus19Service;
    Pus20Service pus20Service;
    Pus22Service pus22Service;

    private final Object linkLock = new Object();
    private volatile boolean los;
    private volatile Date lastLosStart;
    private volatile Date lastLosStop;
    private boolean autoDumpOnAos = true;
    private int losCycleAosSeconds;
    private int losCycleLosSeconds;

    protected BlockingQueue<PusTcPacket> pendingCommands = new ArrayBlockingQueue<>(100);

    @Override
    public void setCfdpSender(CfdpSender cfdpSender) {
        this.cfdpSender = cfdpSender;
    }

    public PusSimulator(File dataDir) {
        powerDataHandler = new PowerHandler();
        rcsHandler = new RCSHandler();
        epslvpduHandler = new EpsLvpduHandler();
        flightDataHandler = new FlightDataHandler();
        dhsHandler = new DHSHandler();
        cfdpReceiver = new CfdpReceiver(this, dataDir);
        pus5Service = new Pus5Service(this);
        pus11Service = new Pus11Service(this);
        pus15Service = new Pus15Service(this);
        pus17Service = new Pus17Service(this);
        pus19Service = new Pus19Service(this);
        pus20Service = new Pus20Service(this);
        pus22Service = new Pus22Service(this);
    }

    @Override
    protected void doStart() {
        executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleAtFixedRate(() -> sendTimePacket(), 0, 4, TimeUnit.SECONDS);

        executor.scheduleAtFixedRate(() -> sendFlightPacket(), 0, 200, TimeUnit.MILLISECONDS);
        executor.scheduleAtFixedRate(() -> sendHkTm(), 0, 1000, TimeUnit.MILLISECONDS);
        // executor.scheduleAtFixedRate(() -> sendCfdp(), 0, 1000, TimeUnit.MILLISECONDS);
        executor.scheduleAtFixedRate(() -> executePendingCommands(), 0, 200, TimeUnit.MILLISECONDS);

        pus5Service.start();
        pus11Service.start();
        pus22Service.start();

        if (losCycleLosSeconds > 0) {
            log.info("Automatic LOS/AOS cycle: {}s AOS, {}s LOS", losCycleAosSeconds, losCycleLosSeconds);
            executor.schedule(this::losCycleStep, losCycleAosSeconds, TimeUnit.SECONDS);
        }
    }

    private void losCycleStep() {
        if (isLOS()) {
            setAOS();
            executor.schedule(this::losCycleStep, losCycleAosSeconds, TimeUnit.SECONDS);
        } else {
            setLOS();
            executor.schedule(this::losCycleStep, losCycleLosSeconds, TimeUnit.SECONDS);
        }
    }

    /**
     * Configure an automatic LOS/AOS cycle starting (in AOS) when the simulator starts. Has to be called before the
     * simulator is started.
     *
     * @param aosSeconds
     *            duration of each AOS
     * @param losSeconds
     *            duration of each LOS; 0 disables the automatic cycle
     */
    public void setLosCycle(int aosSeconds, int losSeconds) {
        if (losSeconds > 0 && aosSeconds <= 0) {
            throw new IllegalArgumentException("aosSeconds has to be positive");
        }
        this.losCycleAosSeconds = aosSeconds;
        this.losCycleLosSeconds = losSeconds;
    }

    /**
     * If true (default), dump the packets stored since the last automatic dump on the LOS link right after AOS.
     */
    public void setAutoDumpOnAos(boolean autoDumpOnAos) {
        this.autoDumpOnAos = autoDumpOnAos;
    }

    @Override
    public boolean isLOS() {
        return los;
    }

    @Override
    public void setLOS() {
        synchronized (linkLock) {
            if (los) {
                return;
            }
            los = true;
            lastLosStart = new Date();
        }
        log.info("LOS: realtime TM and TC stopped, storing TM on-board");
    }

    @Override
    public void setAOS() {
        synchronized (linkLock) {
            if (!los) {
                return;
            }
            los = false;
            lastLosStop = new Date();
        }
        log.info("AOS: realtime TM and TC resumed ({} packets in the packet store)", pus15Service.storedCount());
        if (autoDumpOnAos) {
            pus15Service.autoDump();
        }
    }

    @Override
    public Date getLastLosStart() {
        return lastLosStart;
    }

    @Override
    public Date getLastLosStop() {
        return lastLosStop;
    }

    @Override
    public PowerHandler getPowerHandler() {
        return powerDataHandler;
    }

    private void sendFlightPacket() {
        PusTmPacket packet = new PusTmPacket(MAIN_APID, 4 + flightDataHandler.dataSize(), PUS_TYPE_HK, 25);
        ByteBuffer buffer = packet.getUserDataBuffer();
        buffer.putInt(0);
        flightDataHandler.fillPacket(buffer.slice());
        transmitRealtimeTM(packet);
    }

    private void sendHkTm() {
        try {
            PusTmPacket packet = new PusTmPacket(MAIN_APID, 4 + powerDataHandler.dataSize(), PUS_TYPE_HK, 25);
            ByteBuffer buffer = packet.getUserDataBuffer();
            buffer.putInt(1);
            powerDataHandler.fillPacket(buffer.slice());
            transmitRealtimeTM(packet);

            packet = new PusTmPacket(MAIN_APID, 4 + dhsHandler.dataSize(), PUS_TYPE_HK, 25);
            buffer = packet.getUserDataBuffer();
            buffer.putInt(2);
            dhsHandler.fillPacket(buffer.slice());
            transmitRealtimeTM(packet);

            packet = new PusTmPacket(MAIN_APID, 4 + rcsHandler.dataSize(), PUS_TYPE_HK, 25);
            buffer = packet.getUserDataBuffer();
            buffer.putInt(3);
            rcsHandler.fillPacket(buffer.slice());
            transmitRealtimeTM(packet);

            packet = new PusTmPacket(MAIN_APID, 4 + epslvpduHandler.dataSize(), PUS_TYPE_HK, 25);
            buffer = packet.getUserDataBuffer();
            buffer.putInt(4);
            epslvpduHandler.fillPacket(buffer.slice());
            transmitRealtimeTM(packet);

            // hkid 5: ST[22] simulated orbit position
            packet = new PusTmPacket(MAIN_APID, 4 + PusPosition.LENGTH_BYTES, PUS_TYPE_HK, 25);
            buffer = packet.getUserDataBuffer();
            buffer.putInt(5);
            pus22Service.currentPosition().encode(buffer);
            transmitRealtimeTM(packet);

            // hkid 6: LOS and on-board storage status
            packet = new PusTmPacket(MAIN_APID, 4 + 7, PUS_TYPE_HK, 25);
            buffer = packet.getUserDataBuffer();
            buffer.putInt(6);
            buffer.put((byte) (los ? 1 : 0));
            buffer.put((byte) (pus15Service.storageEnabled ? 1 : 0));
            buffer.putInt(pus15Service.storedCount());
            buffer.put((byte) pus15Service.fillPercentage());
            transmitRealtimeTM(packet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Transmit a packet in realtime if in AOS, store it in the ST[15] packet store if in LOS.
     */
    void transmitRealtimeTM(PusTmPacket packet) {
        packet.fillChecksum();
        byte[] b = packet.getBytes();
        synchronized (linkLock) {
            if (los) {
                pus15Service.storeLosPacket(b);
            } else {
                sendRealtime(b);
            }
        }
    }

    /**
     * Send a packet on the realtime link and frames
     */
    protected void sendRealtime(byte[] packet) {
        if (tmLink != null) {
            tmLink.sendPacket(packet);
        }
        if (tmFrameLink != null) {
            tmFrameLink.queuePacket(0, packet);
        }
    }

    /**
     * Send a stored packet on the dump (LOS) link
     */
    protected void transmitDump(byte[] packet) {
        if (losLink != null) {
            losLink.sendPacket(packet);
        }
    }

    /**
     * @return true if the dump (LOS) link is connected and the simulator is in AOS
     */
    protected boolean isDumpLinkAvailable() {
        return !los && losLink != null && losLink.isConnected();
    }

    private void sendTimePacket() {
        if (los) {
            return;
        }
        PusTmTimePacket packet = new PusTmTimePacket();
        if (tmLink != null) {
            tmLink.sendImmediate(packet);
        }
        if (tmFrameLink != null) {
            // the ST[9] time packet must travel on a virtual channel so the ground can associate it
            // with a frame (and its earth reception time) for time correlation
            tmFrameLink.queuePacket(0, packet.getBytes());
        }
    }

    @Override
    protected void doStop() {
        executor.shutdownNow();
        pus15Service.stop();
    }

    @Override
    public void transmitCfdp(CfdpPacket packet) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void processUplinkTc(SimulatorCcsdsPacket tc) {
        if (los) {
            log.warn("LOS: dropping TC received from the ground (apid: {}, seq: {})", tc.getAPID(),
                    tc.getSequenceCount());
            return;
        }
        processTc(tc);
    }

    @Override
    public void processTc(SimulatorCcsdsPacket tc) {
        if (tc instanceof PusTcPacket pustc) {
            transmitRealtimeTM(ack(pustc, 1));
            try {
                pendingCommands.put(pustc);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            if (tc.getAPID() == CfdpCcsdsPacket.APID) {
                byte b0 = tc.getUserDataBuffer().get();
                if ((b0 & 0x08) == 0) { // towards receiver
                    cfdpReceiver.processCfdp(tc.getUserDataBuffer());
                } else {// towards sender
                    if (cfdpSender != null) {
                        cfdpSender.processCfdp(tc.getUserDataBuffer());
                    } else {
                        log.warn("Received CFDP packet for sender but have no sender");
                    }
                }
            }
        }
    }

    private void switchBatteryOn(PusTcPacket commandPacket) {
        int batNum = commandPacket.getUserDataBuffer().get(0);
        if (batNum < 1 || batNum > 3) {
            log.info("CMD: BATERRY ON {}, sending NACK start", batNum);
            transmitRealtimeTM(nack(commandPacket, 4, START_FAILURE_INVALID_VOLTAGE_NUM));
            return;
        }
        if (batNum != 2) {
            log.info("CMD: BATERRY ON {} ACK start", batNum);
            transmitRealtimeTM(ack(commandPacket, 3));
        } else {
            log.info("CMD: BATERRY ON {}, skip ACK start", batNum);
        }

        executor.schedule(() -> {
            if (batNum == 3) {
                int returnCode = random.nextInt(5);
                log.info("CMD: BATERRY ON {}, sending failure completion with code {}", batNum, returnCode);
                transmitRealtimeTM(nack(commandPacket, 8, returnCode));
            } else {
                powerDataHandler.setBatteryOn(batNum);
                transmitRealtimeTM(ack(commandPacket, 7));
            }
        }, 1500, TimeUnit.MILLISECONDS);
    }

    private void switchBatteryOff(PusTcPacket commandPacket) {
        transmitRealtimeTM(ack(commandPacket, 3));
        int batNum = commandPacket.getUserDataBuffer().get(0);
        log.info("CMD: BATERRY OFF {}", batNum);
        executor.schedule(() -> {
            powerDataHandler.setBatteryOff(batNum);
            transmitRealtimeTM(ack(commandPacket, 7));
        }, 500, TimeUnit.MILLISECONDS);
    }

    private void executePendingCommands() {
        PusTcPacket commandPacket;
        while ((commandPacket = pendingCommands.poll()) != null) {
            try {
                log.info("Received PUS TC : {} (now: {})", commandPacket, PusTime.now());
                switch (commandPacket.getType()) {
                case 5 -> pus5Service.executeTc(commandPacket);
                case 11 -> pus11Service.executeTc(commandPacket);
                case 15 -> pus15Service.executeTc(commandPacket);
                case 17 -> pus17Service.executeTc(commandPacket);
                case 19 -> pus19Service.executeTc(commandPacket);
                case 20 -> pus20Service.executeTc(commandPacket);
                case 22 -> pus22Service.executeTc(commandPacket);
                case 25 -> {
                    switch (commandPacket.getSubtype()) {
                    case 1 -> switchBatteryOn(commandPacket);
                    case 2 -> switchBatteryOff(commandPacket);
                    default -> log.error("Invalid command  subtype {}", commandPacket.getSubtype());
                    }
                }
                default -> log.warn("Unknown command type {}", commandPacket.getType());
                }
            } catch (Exception e) {
                log.warn("Error executing command", e);
            }
        }
    }

    protected static PusTmPacket ack(PusTcPacket commandPacket, int subtype) {
        PusTmPacket ackPacket = new PusTmPacket(MAIN_APID, 4, PUS_TYPE_ACK, subtype);

        ByteBuffer bb = ackPacket.getUserDataBuffer();
        bb.put(commandPacket.getBytes(), 0, 4);
        return ackPacket;
    }

    public static PusTmPacket nack(PusTcPacket commandPacket, int subtype, int code) {
        return nack(commandPacket, subtype, code, new byte[0]);
    }

    /**
     * Failure verification report with failure data (the data of the failure notice) after the failure code
     */
    public static PusTmPacket nack(PusTcPacket commandPacket, int subtype, int code, byte[] failureData) {
        PusTmPacket ackPacket = new PusTmPacket(MAIN_APID, 8 + failureData.length, PUS_TYPE_ACK, subtype);

        ByteBuffer bb = ackPacket.getUserDataBuffer();
        bb.put(commandPacket.getBytes(), 0, 4);
        bb.putInt(code);
        bb.put(failureData);
        return ackPacket;
    }

    @Override
    protected void setTmLink(TcpTmTcLink tmLink) {
        this.tmLink = tmLink;
    }

    @Override
    public void setTmFrameLink(UdpTmFrameLink tmFrameLink) {
        this.tmFrameLink = tmFrameLink;
    }

    @Override
    protected void setTm2Link(TcpTmTcLink tm2Link) {
        // ignore only send packets on tmlink
    }

    @Override
    protected void setLosLink(TcpTmTcLink losLink) {
        this.losLink = losLink;
    }

    @Override
    public int maxTmDataSize() {
        return 1500;
    }
}