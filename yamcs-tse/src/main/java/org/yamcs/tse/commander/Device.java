package org.yamcs.tse.commander;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class Device {

    private String id;
    private String description;

    protected String responseTermination;
    protected long responseTimeout = 3000;

    protected Charset encoding = StandardCharsets.US_ASCII;

    public Device(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getResponseTermination() {
        return responseTermination;
    }

    public void setResponseTermination(String responseTermination) {
        this.responseTermination = responseTermination;
    }

    public long getResponseTimeout() {
        return responseTimeout;
    }

    public void setResponseTimeout(long responseTimeout) {
        this.responseTimeout = responseTimeout;
    }

    public String command(String cmd) throws IOException, InterruptedException {
        System.out.format("%s <<< %s\n", id, cmd);
        write(cmd);
        if (cmd.contains("?") || cmd.contains("!")) { // Should maybe make this configurable
            String response = read();
            if (response != null) {
                System.out.format("%s >>> %s\n", id, response);
            }
            return response;
        }
        return null;
    }

    public abstract void connect() throws IOException;

    public abstract void disconnect() throws IOException;

    public abstract void write(String cmd) throws IOException;

    public abstract String read() throws IOException, InterruptedException;
}
