package prerna.reactor.workflowtool;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public final class WorkflowBlockStatusUtil {
    public static final String BLOCK_TABLE_NAME = "workflowblocks";
    public static final String BLOCK_PREFIX = BLOCK_TABLE_NAME + "__";

    public static final String[] BLOCK_COLUMN_NAMES = new String[] { "block_id", "guid", "sender_id", "receiver_id",
            "current_stage", "previous_stage", "workflow_notes", "mod_date", "notes", "is_assigned", "is_latest" };

    public static enum BLOCK_STAGES {
        STAGE1("STAGE 1"), STAGE2("STAGE 2"), STAGE3("STAGE 3"), STAGE4("STAGE 4"), STAGE5("STAGE 5"),
        COMPLETE("COMPLETE"), SYSTEM("SYSTEM");

        private String stage;

        private BLOCK_STAGES(String stage) {
            this.stage = stage;
        }

        public String getStage() {
            return this.stage;
        }
    }

    public static enum BLOCK_STATUSES {
        ASSIGNED("TRUE"), UNASSIGNED("FALSE");

        private String status;

        private BLOCK_STATUSES(String status) {
            this.status = status;
        }

        public String getStatus() {
            return this.status;
        }
    }

    // read only list for BLOCK stages
    public static final List<String> BLOCK_STAGES_LIST = Stream.of(BLOCK_STAGES.values()).map(BLOCK_STAGES::name)
            .collect(Collectors.toList());

    // read only list for indexing help
    public static final List<String> BLOCK_COLUMN_NAME_LIST = Arrays.asList(BLOCK_COLUMN_NAMES);

    /**
     * Create the prepared statement insert
     * 
     * @param columnNames
     * @return
     */
    public static String createInsertPreparedStatementSql() {
        StringBuilder ps = new StringBuilder();
        ps.append("INSERT INTO ").append(BLOCK_TABLE_NAME).append(" (").append(BLOCK_COLUMN_NAMES[0]);
        for (int i = 1; i < BLOCK_COLUMN_NAMES.length; i++) {
            ps.append(", ").append(BLOCK_COLUMN_NAMES[i]);
        }
        ps.append(") VALUES (?");
        for (int i = 1; i < BLOCK_COLUMN_NAMES.length; i++) {
            ps.append(", ?");
        }
        ps.append(")");
        return ps.toString();
    }

    /**
     * Create the prepared statement update
     * 
     * @param columnNames
     * @return
     */
    public static String createUpdatePreparedStatementSql() {
        return "UPDATE " + BLOCK_TABLE_NAME + " SET is_latest=false where block_id=? and guid=? and is_latest=true";
    }

    public static String createUpdatePreparedStatementSqlNoGuid() {
        return "UPDATE " + BLOCK_TABLE_NAME + " SET is_latest=false where block_id=? and is_latest=true";
    }
    
    /**
     * query struct with selectors
     */
    public static SelectQueryStruct getAllWorkflowSelectorQs() {
        SelectQueryStruct qs = new SelectQueryStruct();
        for (String columnName : BLOCK_COLUMN_NAMES) {
               qs.addSelector(new QueryColumnSelector(BLOCK_PREFIX + columnName));
        }

        return qs;
    }

    private WorkflowBlockStatusUtil() {

    }
}
