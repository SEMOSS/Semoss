package prerna.reactor.workflow;

/**
 * Shared constants for the Workflow Engine subsystem.
 * Covers table/column names, status values, and node types.
 */
public class WorkflowConstants {

    private WorkflowConstants() {}

    // ── File names ────────────────────────────────────────────────────────────────

    public static final String WORKFLOW_FILE_NAME = "workflow.json";
    public static final String WORKFLOW_CONFIG_FILE_NAME = "workflow-config.json";

    // ── DB Table names ────────────────────────────────────────────────────────────

    public static final String TABLE_WORKFLOW_RUNS = "WORKFLOW_RUNS";
    public static final String TABLE_WORKFLOW_NODE_OUTPUTS = "WORKFLOW_NODE_OUTPUTS";
    public static final String TABLE_WORKFLOW_FOREACH_ROWS = "WORKFLOW_FOREACH_ROWS";

    // ── WORKFLOW_RUNS columns ─────────────────────────────────────────────────────

    public static final String RUN_ID = "RUN_ID";
    public static final String PROJECT_ID = "PROJECT_ID";
    public static final String WORKFLOW_ID = "WORKFLOW_ID";
    public static final String STATUS = "STATUS";
    public static final String TRIGGER_TYPE = "TRIGGER_TYPE";
    public static final String RESUMED_FROM_RUN = "RESUMED_FROM_RUN";
    public static final String STARTED_AT = "STARTED_AT";
    public static final String COMPLETED_AT = "COMPLETED_AT";
    public static final String FAILED_NODE_ID = "FAILED_NODE_ID";
    public static final String ERROR_MESSAGE = "ERROR_MESSAGE";
    public static final String LAST_HEARTBEAT = "LAST_HEARTBEAT";
    public static final String TOTAL_NODES = "TOTAL_NODES";
    public static final String COMPLETED_NODES = "COMPLETED_NODES";
    public static final String CREATED_BY = "CREATED_BY";
    public static final String PARENT_RUN_ID = "PARENT_RUN_ID";
    public static final String PARENT_NODE_ID = "PARENT_NODE_ID";

    // ── WORKFLOW_NODE_OUTPUTS columns ─────────────────────────────────────────────

    public static final String NODE_ID = "NODE_ID";
    public static final String NODE_LABEL = "NODE_LABEL";
    public static final String EXECUTION_ORDER = "EXECUTION_ORDER";
    public static final String DURATION_MS = "DURATION_MS";
    public static final String OUTPUT_VAR = "OUTPUT_VAR";
    public static final String OUTPUT_VALUE = "OUTPUT_VALUE";
    public static final String OUTPUT_PREVIEW = "OUTPUT_PREVIEW";
    public static final String ROW_COUNT = "ROW_COUNT";

    // ── WORKFLOW_FOREACH_ROWS columns ─────────────────────────────────────────────

    public static final String ROW_INDEX = "ROW_INDEX";
    public static final String ROW_KEY = "ROW_KEY";

    // ── Run statuses ──────────────────────────────────────────────────────────────

    public static final String STATUS_RUNNING = "RUNNING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_INTERRUPTED = "INTERRUPTED";
    public static final String STATUS_CANCELLED = "CANCELLED";

    // ── Node statuses ─────────────────────────────────────────────────────────────

    public static final String NODE_STATUS_PENDING = "PENDING";
    public static final String NODE_STATUS_RUNNING = "RUNNING";
    public static final String NODE_STATUS_SUCCESS = "SUCCESS";
    public static final String NODE_STATUS_FAILED = "FAILED";
    public static final String NODE_STATUS_SKIPPED = "SKIPPED";

    // ── Trigger types ─────────────────────────────────────────────────────────────

    public static final String TRIGGER_MANUAL = "MANUAL";
    public static final String TRIGGER_SCHEDULED = "SCHEDULED";
    public static final String TRIGGER_RESUME = "RESUME";
    public static final String TRIGGER_SUB_WORKFLOW = "SUB_WORKFLOW";

    // ── Node types ────────────────────────────────────────────────────────────────

    public static final String NODE_TRIGGER = "trigger";
    public static final String NODE_DATABASE_ENGINE = "database-engine";
    public static final String NODE_STORAGE_ENGINE = "storage-engine";
    public static final String NODE_VECTOR_ENGINE = "vector-engine";
    public static final String NODE_MODEL_ENGINE = "model-engine";
    public static final String NODE_FUNCTION_ENGINE = "function-engine";
    public static final String NODE_APP = "app";
    public static final String NODE_CUSTOM_PIXEL = "custom-pixel";
    public static final String NODE_FOR_EACH = "for-each";
    public static final String NODE_TRANSFORM = "transform";
    public static final String NODE_SUB_WORKFLOW = "sub-workflow";

    // ── Sub-workflow node config keys ─────────────────────────────────────────────

    public static final String SUB_WORKFLOW_TARGET_PROJECT = "targetProjectId";
    public static final String SUB_WORKFLOW_INPUT_MAPPING = "inputMapping";
    public static final int MAX_SUB_WORKFLOW_DEPTH = 10;

    // ── Data type constants (for table creation) ──────────────────────────────────

    public static final String VARCHAR_255 = "VARCHAR(255)";
    public static final String VARCHAR_500 = "VARCHAR(500)";
    public static final String VARCHAR_1000 = "VARCHAR(1000)";
    public static final String VARCHAR_2000 = "VARCHAR(2000)";
    public static final String INTEGER = "INTEGER";
    public static final String BIGINT = "BIGINT";
    public static final String NOT_NULL = "NOT NULL";

    // ── Defaults ──────────────────────────────────────────────────────────────────

    public static final String DEFAULT_WORKFLOW_ID = "default";
    public static final int DEFAULT_TIMEOUT_SECONDS = 300;
    public static final int HEARTBEAT_INTERVAL_SECONDS = 30;
    public static final int STALE_HEARTBEAT_THRESHOLD_MINUTES = 5;
    public static final int FOREACH_BATCH_SIZE = 100;
    public static final int OUTPUT_PREVIEW_MAX_LENGTH = 2000;

    // ── Legacy (kept for backward-compat with run-history.json fallback) ──────────

    /** @deprecated Use TABLE_WORKFLOW_RUNS instead */
    @Deprecated
    public static final String WORKFLOW_RUN_HISTORY = "WORKFLOW_RUN_HISTORY";
    /** @deprecated Use PROJECT_ID instead */
    @Deprecated
    public static final String APP_ID = "APP_ID";
    /** @deprecated Use ERROR_MESSAGE instead */
    @Deprecated
    public static final String ERROR = "ERROR";
}
