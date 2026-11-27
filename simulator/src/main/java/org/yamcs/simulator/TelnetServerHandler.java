package org.yamcs.simulator;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class TelnetServerHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger log = LoggerFactory.getLogger(TelnetServerHandler.class);

    private AbstractSimulator simulator;

    public TelnetServerHandler(AbstractSimulator simulator) {
        this.simulator = simulator;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Telnet client connected: " + ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String scpi) throws Exception {
        String[] commands = scpi.trim().split(";");
        List<String> responses = new ArrayList<>();

        for (String command : commands) {
            if (command.trim().isEmpty()) {
                continue;
            }

            String[] parts = command.trim().split("\\s+", 2);
            try {
                execute(parts[0].toUpperCase(), responses);
            } catch (UnsupportedOperationException e) {
                responses.add(e.getMessage());
            }
        }

        if (!responses.isEmpty()) {
            ctx.write(String.join(";", responses));
            ctx.writeAndFlush("\r\n");
        }
    }

    private void execute(String command, List<String> responses) {
        switch (command) {
        case "*IDN?":
            responses.add("SPACEAPPS,Demo Simulator");
            break;
        case ":BATTERY1:VOLTAGE?":
            responses.add(batteryVoltage(1));
            break;
        case ":BATTERY2:VOLTAGE?":
            responses.add(batteryVoltage(2));
            break;
        case ":BATTERY3:VOLTAGE?":
            responses.add(batteryVoltage(3));
            break;
        case ":LOS?":
            responses.add((simulator.isLOS() ? "1" : "0"));
            break;
        case ":LOS:STAR":
        case ":LOS:START":
        case "LOS":
            simulator.setLOS();
            break;
        case ":LOS:STOP":
        case "AOS":
            simulator.setAOS();
            break;
        case ":LOS:START:DATE?":
        case ":LOS:STAR:DATE?":
            Date startDate = simulator.getLastLosStart();
            if (startDate == null) {
                responses.add("");
            } else {
                responses.add(formatDate(startDate));
            }
            break;
        case ":LOS:START:TIME?":
        case ":LOS:STAR:TIME?":
            Date startTime = simulator.getLastLosStart();
            if (startTime == null) {
                responses.add("");
            } else {
                responses.add(formatTime(startTime));
            }
            break;
        case ":LOS:STOP:DATE?":
            Date stopDate = simulator.getLastLosStop();
            if (stopDate == null) {
                responses.add("");
            } else {
                responses.add(formatDate(stopDate));
            }
            break;
        case ":LOS:STOP:TIME?":
            Date stopTime = simulator.getLastLosStop();
            if (stopTime == null) {
                responses.add("");
            } else {
                responses.add(formatTime(stopTime));
            }
            break;
        case ":DATE?":
            responses.add(formatDate(new Date()));
            break;
        case ":TIME?":
            responses.add(formatTime(new Date()));
            break;
        case "HELP":
            responses.add("LOS | AOS | :LOS? | :LOS:START | :LOS:STOP | :LOS:START:DATE? | :LOS:START:TIME? | "
                    + ":LOS:STOP:DATE? | :LOS:STOP:TIME? | :BATTERYn:VOLTAGE? | :DATE? | :TIME? | *IDN?");
            break;
        default:
            responses.add("unrecognized command");
        }
    }

    private String batteryVoltage(int batNum) {
        PowerHandler ph = simulator.getPowerHandler();
        if (ph == null) {
            return "";
        }
        return Float.toString(switch (batNum) {
        case 1 -> ph.getBattery1Voltage();
        case 2 -> ph.getBattery2Voltage();
        default -> ph.getBattery3Voltage();
        });
    }

    private static String formatDate(Date date) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy,MM,dd");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(date);
    }

    private static String formatTime(Date date) {
        SimpleDateFormat df = new SimpleDateFormat("HH,mm,ss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(date);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Closing channel due to exception", cause);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Telnet client disconnected: " + ctx.channel().remoteAddress());
    }
}
