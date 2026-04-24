package prerna.util.jobrunr.model;

/**
 * Enum representing the status of a scheduled job.
 */
public enum JobStatus {
    /**
     * Job is actively scheduled and will execute according to its cron expression.
     */
    ACTIVE,
    
    /**
     * Job is paused and will not execute until resumed.
     */
    PAUSED
}
