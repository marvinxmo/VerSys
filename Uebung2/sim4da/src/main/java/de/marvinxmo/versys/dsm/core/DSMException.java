package de.marvinxmo.versys.dsm.core;

/**
 * Exception thrown by DSM operations when they fail
 */
public class DSMException extends Exception {
    private final ErrorType errorType;

    public enum ErrorType {
        QUORUM_NOT_AVAILABLE("Insufficient nodes available for quorum"),
        NETWORK_PARTITION("Network partition detected"),
        TIMEOUT("Operation timed out"),
        CONSISTENCY_VIOLATION("Consistency constraint violated"),
        NODE_UNAVAILABLE("Target node is unavailable"),
        INVALID_OPERATION("Invalid operation requested");

        private final String description;

        ErrorType(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public DSMException(ErrorType errorType, String message) {
        super(message);
        this.errorType = errorType;
    }

    public DSMException(ErrorType errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    @Override
    public String toString() {
        return String.format("DSMException[%s]: %s", errorType, getMessage());
    }
}
