package prerna.nodemesh.reactors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.nodemesh.db.Queries;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GetWorkflowTriggersReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(GetWorkflowTriggersReactor.class);

    public GetWorkflowTriggersReactor() {
        this.keysToGet = new String[]{
                "workflow_id",
                "type" // optional filter
        };
        this.keyRequired = new int[]{1, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        // Auth check (keeps parity with other reactors)
        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }

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

        String rawTypeFilter = this.keyValue.get(this.keysToGet[1]);
        final String typeFilter = (rawTypeFilter == null) ? null : rawTypeFilter.trim();

        List<Map<String, Object>> rows = Queries.getWorkflowTriggers(workflowId);

        if (typeFilter != null && !typeFilter.isEmpty()) {
            rows = rows.stream()
                    .filter(r -> {
                        Object t = r.get("type");
                        return t != null && typeFilter.equalsIgnoreCase(String.valueOf(t));
                    })
                    .collect(Collectors.toList());
        }

        logger.info("Fetched {} trigger(s) for workflow_id={} (typeFilter={}) by user={}",
                rows.size(), workflowId, typeFilter, user.getPrimaryLoginToken().getId());

        // List<Map<String,Object>> naturally maps to a tabular structure
        return new NounMetadata(rows, PixelDataType.JSON_OBJECT);
    }
}
