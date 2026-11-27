package org.yamcs.simulator;

import java.util.Date;

import org.yamcs.cfdp.pdu.CfdpPacket;

import com.google.common.util.concurrent.AbstractService;
import org.yamcs.simulator.cfdp.CfdpSender;

public abstract class AbstractSimulator extends AbstractService {
    protected abstract void processTc(SimulatorCcsdsPacket tc);

    /**
     * Called by the TC links (TCP, TC frames, USLP frames) for a TC received from the ground.
     * <p>
     * Simulators that model the loss of signal override this to drop the TC during LOS. TCs generated on-board (for
     * example released from a schedule) go directly to {@link #processTc(SimulatorCcsdsPacket)}.
     */
    protected void processUplinkTc(SimulatorCcsdsPacket tc) {
        processTc(tc);
    }

    /**
     * @return true if the simulator is in loss of signal
     */
    public boolean isLOS() {
        return false;
    }

    /**
     * Enter loss of signal: stop the realtime downlink and the uplink.
     */
    public void setLOS() {
        throw new UnsupportedOperationException("LOS not supported by " + getClass().getSimpleName());
    }

    /**
     * Acquisition of signal: resume the realtime downlink and the uplink.
     */
    public void setAOS() {
        throw new UnsupportedOperationException("AOS not supported by " + getClass().getSimpleName());
    }

    public Date getLastLosStart() {
        return null;
    }

    public Date getLastLosStop() {
        return null;
    }

    /**
     * @return the power data handler used for the telnet battery queries or null if the simulator has none
     */
    public PowerHandler getPowerHandler() {
        return null;
    }

    protected abstract void setTmLink(TcpTmTcLink tmLink);

    protected abstract void setTm2Link(TcpTmTcLink tm2Link);

    protected abstract void setLosLink(TcpTmTcLink losLink);

    /**
     * Set the TM frame link. Simulators that support sending their telemetry as CCSDS transfer frames override this and
     * feed {@link UdpTmFrameLink#queuePacket}. The default is a no-op (packet-only simulator).
     */
    public void setTmFrameLink(UdpTmFrameLink tmFrameLink) {
        // no frame support by default
    }

    public abstract void transmitCfdp(CfdpPacket packet);

    public void setCfdpSender(CfdpSender cfdpSender) {
        throw new UnsupportedOperationException("CfdpSender is not settable for " + this);
    }

    public abstract int maxTmDataSize();

}
