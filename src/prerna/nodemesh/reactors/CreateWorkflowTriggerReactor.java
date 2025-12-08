package prerna.nodemesh.reactors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.nodemesh.db.Queries;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CreateWorkflowTriggerReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(CreateWorkflowTriggerReactor.class);

    public CreateWorkflowTriggerReactor() {
        this.keysToGet = new String[]{
                "workflow_id",
                "type",
                "config" // optional JSON string
        };
        this.keyRequired = new int[]{1, 1, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        // Ensure user is authenticated (mirrors your workflow reactor)
        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }

        // Required: workflow_id
        String workflowIdStr = this.keyValue.get(this.keysToGet[0]);
        if (workflowIdStr == null || workflowIdStr.trim().isEmpty()) {
            throw new IllegalArgumentException("workflow_id is required");
        }

        int workflowId;
        try {
            workflowId = Integer.parseInt(workflowIdStr.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("workflow_id must be an integer", e);
        }

        // Required: type
        String type = this.keyValue.get(this.keysToGet[1]);
        if (type == null || type.trim().isEmpty()) {
            throw new IllegalArgumentException("type is required (e.g., 'cron', 'webhook', 'event', 'manual')");
        }

        // Optional: config (JSON)
        String config = this.keyValue.get(this.keysToGet[2]);
        if (config == null || config.trim().isEmpty()) {
            config = "{}";
        }

        int triggerId = Queries.createWorkflowTrigger(workflowId, type, config);
        if (triggerId == -1) {
            throw new RuntimeException("Failed to create workflow trigger");
        }

        logger.info("Created workflow trigger id={} for workflow_id={} by user={}",
                triggerId, workflowId, user.getPrimaryLoginToken().getId());

        return new NounMetadata(triggerId, PixelDataType.CONST_INT);
    }
}
