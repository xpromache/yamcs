package org.yamcs.obsched;

import java.util.Map;
import java.util.UUID;

import org.yamcs.security.User;

/**
 * What the {@link OnboardSchedulerService} offers to the {@link SchedulerAdapter} during one sync.
 * <p>
 * Commands are sent on behalf of the user who started the sync, so the usual commanding privileges apply.
 */
public interface SyncContext {

    User getUser();

    /**
     * Sends a command that is not an entry upload, for example a request to delete activities or to report the
     * schedule.
     *
     * @param command
     *            qualified name of the command
     * @param args
     *            argument values
     * @param options
     *            command options (command history attributes), may be empty
     */
    IssuedCommand sendCommand(String command, Map<String, Object> args, Map<String, Object> options);

    /**
     * Uploads an entry: sends the command of the entry with the given options (for example the options that make the
     * post-processor wrap the command into a schedule insert request).
     * <p>
     * The command verifiers of the entry command are disabled for the upload. The entry becomes UPLOAD_PENDING and
     * the upload is recorded in its history. The adapter has to call {@link #setUploadRef} once it knows the on-board
     * reference and report the confirmation of the insert as status ONBOARD.
     */
    IssuedCommand uploadEntry(UUID entryId, Map<String, Object> options);

    /**
     * Sets the on-board reference of an upload.
     */
    void setUploadRef(UUID entryId, IssuedCommand upload, String ref);

    /**
     * Confirms that the on-board copy with the given reference has been deleted.
     * <p>
     * An entry with intent DELETE_PENDING becomes DELETED; an entry that was deleted to be replaced by a newer
     * revision becomes NOT_ONBOARD.
     */
    void confirmDeleted(UUID entryId, String ref);

    /**
     * Reports that an upload has been rejected (on the ground or on board). The entry becomes NOT_ONBOARD.
     */
    void uploadFailed(UUID entryId, String reason);
}
