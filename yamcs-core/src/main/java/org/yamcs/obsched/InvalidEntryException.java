package org.yamcs.obsched;

/**
 * Thrown when a schedule entry cannot be created, changed or deleted.
 */
@SuppressWarnings("serial")
public class InvalidEntryException extends Exception {
    public InvalidEntryException(String message) {
        super(message);
    }
}
