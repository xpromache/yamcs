package org.yamcs.obsched;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.yamcs.protobuf.obsched.EntryIntent;
import org.yamcs.protobuf.obsched.EntryState;
import org.yamcs.protobuf.obsched.EvidenceInfo;
import org.yamcs.protobuf.obsched.EvidenceSource;
import org.yamcs.protobuf.obsched.ScheduleEntryInfo;
import org.yamcs.protobuf.obsched.UploadInfo;
import org.yamcs.utils.TimeEncoding;

import com.google.protobuf.Struct;

/**
 * Ground model of one time-tagged command of an on-board schedule.
 * <p>
 * An entry has two independent state axes: the {@link EntryIntent} (what the ground wants) and the {@link EntryState}
 * (what is known about the on-board copy). See {@code docs/design/onboard-scheduler.md}.
 * <p>
 * Instances are not thread safe; the {@link OnboardSchedulerService} accesses them from its own executor only and
 * hands out copies.
 */
public class ScheduleEntry {

    /**
     * Order of the "forward" states. Evidence can only move an entry to a higher rank, so that late or out of order
     * telemetry (e.g. from dumps) does not regress the state.
     */
    static int rank(EntryState state) {
        return switch (state) {
        case NOT_ONBOARD, MISSED -> 0;
        case UPLOAD_PENDING -> 1;
        case ONBOARD, UNKNOWN -> 2;
        case SKIPPED -> 3;
        case RELEASED -> 4;
        case STARTED -> 5;
        case COMPLETED, FAILED -> 6;
        };
    }

    private final UUID id;
    private final String scheduler;
    private String command;
    private Struct args = Struct.getDefaultInstance();
    private long releaseTime;
    private Struct options = Struct.getDefaultInstance();
    private String description;
    private List<String> tags = new ArrayList<>();
    private int revision = 1;
    private int uploadedRevision = 0;
    private EntryIntent intent = EntryIntent.PLANNED;
    private EntryState state = EntryState.NOT_ONBOARD;
    private String ref;
    private String stateMessage;
    private long created;
    private String createdBy;
    private long modified;
    private String modifiedBy;
    private List<UploadInfo> uploads = new ArrayList<>();
    private List<EvidenceInfo> evidence = new ArrayList<>();

    public ScheduleEntry(UUID id, String scheduler) {
        this.id = id;
        this.scheduler = scheduler;
    }

    public ScheduleEntry(ScheduleEntryInfo info) {
        this.id = UUID.fromString(info.getId());
        this.scheduler = info.getScheduler();
        this.command = info.getCommand();
        this.args = info.getArgs();
        this.releaseTime = TimeEncoding.fromProtobufTimestamp(info.getReleaseTime());
        this.options = info.getOptions();
        this.description = info.hasDescription() ? info.getDescription() : null;
        this.tags = new ArrayList<>(info.getTagsList());
        this.revision = info.getRevision();
        this.uploadedRevision = info.getUploadedRevision();
        this.intent = info.getIntent();
        this.state = info.getState();
        this.ref = info.hasRef() ? info.getRef() : null;
        this.stateMessage = info.hasStateMessage() ? info.getStateMessage() : null;
        this.created = info.hasCreated() ? TimeEncoding.fromProtobufTimestamp(info.getCreated()) : 0;
        this.createdBy = info.hasCreatedBy() ? info.getCreatedBy() : null;
        this.modified = info.hasModified() ? TimeEncoding.fromProtobufTimestamp(info.getModified()) : 0;
        this.modifiedBy = info.hasModifiedBy() ? info.getModifiedBy() : null;
        this.uploads = new ArrayList<>(info.getUploadsList());
        this.evidence = new ArrayList<>(info.getEvidenceList());
    }

    public ScheduleEntry copy() {
        return new ScheduleEntry(toProto(true));
    }

    public ScheduleEntryInfo toProto(boolean details) {
        var b = ScheduleEntryInfo.newBuilder()
                .setId(id.toString())
                .setScheduler(scheduler)
                .setCommand(command)
                .setArgs(args)
                .setReleaseTime(TimeEncoding.toProtobufTimestamp(releaseTime))
                .setOptions(options)
                .addAllTags(tags)
                .setRevision(revision)
                .setUploadedRevision(uploadedRevision)
                .setIntent(intent)
                .setState(state)
                .addAllUploads(uploads);
        if (description != null) {
            b.setDescription(description);
        }
        if (ref != null) {
            b.setRef(ref);
        }
        if (stateMessage != null) {
            b.setStateMessage(stateMessage);
        }
        if (created != 0) {
            b.setCreated(TimeEncoding.toProtobufTimestamp(created));
        }
        if (createdBy != null) {
            b.setCreatedBy(createdBy);
        }
        if (modified != 0) {
            b.setModified(TimeEncoding.toProtobufTimestamp(modified));
        }
        if (modifiedBy != null) {
            b.setModifiedBy(modifiedBy);
        }
        if (details) {
            b.addAllEvidence(evidence);
        } else if (!evidence.isEmpty()) {
            b.addEvidence(evidence.get(evidence.size() - 1));
        }
        return b.build();
    }

    /**
     * Applies a state implied by evidence, if it moves the entry forward. The evidence is always recorded.
     *
     * @return true if the state changed
     */
    boolean applyEvidence(long time, long receptionTime, EvidenceSource source, EntryState newState, String message) {
        addEvidence(time, receptionTime, source, newState, message);
        if (newState == null || newState == state) {
            return false;
        }
        if (rank(newState) > rank(state) || state == EntryState.UNKNOWN && rank(newState) >= rank(state)) {
            state = newState;
            stateMessage = message;
            return true;
        }
        return false;
    }

    /**
     * Sets the state unconditionally (used by reconciliation and by the ground actions), recording it as evidence.
     */
    void forceState(long time, EvidenceSource source, EntryState newState, String message) {
        addEvidence(time, time, source, newState, message);
        state = newState;
        stateMessage = message;
    }

    void addEvidence(long time, long receptionTime, EvidenceSource source, EntryState newState, String message) {
        var eb = EvidenceInfo.newBuilder()
                .setTime(TimeEncoding.toProtobufTimestamp(time))
                .setReceptionTime(TimeEncoding.toProtobufTimestamp(receptionTime))
                .setSource(source);
        if (newState != null) {
            eb.setState(newState);
        }
        if (message != null) {
            eb.setMessage(message);
        }
        evidence.add(eb.build());
    }

    void addUpload(String commandId, long time) {
        uploads.add(UploadInfo.newBuilder()
                .setCommandId(commandId)
                .setTime(TimeEncoding.toProtobufTimestamp(time))
                .setRevision(revision)
                .build());
    }

    /**
     * Sets the on-board reference of the latest upload.
     */
    void setUploadRef(String commandId, String ref) {
        for (int i = uploads.size() - 1; i >= 0; i--) {
            var u = uploads.get(i);
            if (u.getCommandId().equals(commandId)) {
                uploads.set(i, u.toBuilder().setRef(ref).build());
                if (i == uploads.size() - 1) {
                    this.ref = ref;
                }
                return;
            }
        }
    }

    /**
     * @return true if the given reference designates one of the uploaded copies of this entry
     */
    public boolean hasRef(String ref) {
        for (var u : uploads) {
            if (ref.equals(u.getRef())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the command history id of the upload with the given ref, or null
     */
    String getUploadCommandId(String ref) {
        for (int i = uploads.size() - 1; i >= 0; i--) {
            var u = uploads.get(i);
            if (ref.equals(u.getRef())) {
                return u.getCommandId();
            }
        }
        return null;
    }

    /**
     * @return the latest upload or null
     */
    UploadInfo getLatestUpload() {
        return uploads.isEmpty() ? null : uploads.get(uploads.size() - 1);
    }

    /**
     * @return true if the entry has been released or has reached a later state; such entries cannot be edited or
     *         deleted anymore
     */
    public boolean isReleased() {
        return rank(state) >= rank(EntryState.RELEASED) || state == EntryState.SKIPPED;
    }

    /**
     * @return true if a copy of the entry is (or may be) in the on-board schedule
     */
    public boolean isOnboard() {
        return state == EntryState.ONBOARD || state == EntryState.UPLOAD_PENDING;
    }

    public UUID getId() {
        return id;
    }

    public String getScheduler() {
        return scheduler;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Struct getArgs() {
        return args;
    }

    public void setArgs(Struct args) {
        this.args = args;
    }

    public long getReleaseTime() {
        return releaseTime;
    }

    public void setReleaseTime(long releaseTime) {
        this.releaseTime = releaseTime;
    }

    public Struct getOptions() {
        return options;
    }

    public void setOptions(Struct options) {
        this.options = options;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getTags() {
        return Collections.unmodifiableList(tags);
    }

    public void setTags(List<String> tags) {
        this.tags = new ArrayList<>(tags);
    }

    public int getRevision() {
        return revision;
    }

    void incrementRevision() {
        revision++;
    }

    public int getUploadedRevision() {
        return uploadedRevision;
    }

    void setUploadedRevision(int uploadedRevision) {
        this.uploadedRevision = uploadedRevision;
    }

    /**
     * @return true if the on-board copy (if any) is older than the current revision
     */
    public boolean isModifiedSinceUpload() {
        return uploadedRevision != revision;
    }

    public EntryIntent getIntent() {
        return intent;
    }

    void setIntent(EntryIntent intent) {
        this.intent = intent;
    }

    public EntryState getState() {
        return state;
    }

    public String getRef() {
        return ref;
    }

    void setRef(String ref) {
        this.ref = ref;
    }

    public String getStateMessage() {
        return stateMessage;
    }

    void setCreated(long created, String createdBy) {
        this.created = created;
        this.createdBy = createdBy;
        this.modified = created;
        this.modifiedBy = createdBy;
    }

    void setModified(long modified, String modifiedBy) {
        this.modified = modified;
        this.modifiedBy = modifiedBy;
    }

    public List<UploadInfo> getUploads() {
        return Collections.unmodifiableList(uploads);
    }

    public List<EvidenceInfo> getEvidence() {
        return Collections.unmodifiableList(evidence);
    }

    @Override
    public String toString() {
        return "ScheduleEntry[" + id + " " + command + " @" + TimeEncoding.toString(releaseTime) + " " + intent + "/"
                + state + (ref == null ? "" : " ref=" + ref) + "]";
    }
}
