package prerna.nodemesh.reactors;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.auth.User;
import prerna.nodemesh.db.Queries;

public class CreateUserWorkflowReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(CreateUserWorkflowReactor.class);

    public CreateUserWorkflowReactor() {
        this.keysToGet = new String[]{
                "name",
                "description",
        };
        this.keyRequired = new int[]{1, 1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }

        String name = this.keyValue.get(this.keysToGet[0]);
        String description = this.keyValue.get(this.keysToGet[1]);

        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Workflow name is required");
        }

        if (description == null || description.trim().isEmpty()) {
            description = "";
        }

        String createdBy = user.getPrimaryLoginToken().getId();

        int workflowId = Queries.createWorkflow(name, description, createdBy);

        if (workflowId == -1) {
            throw new RuntimeException("Failed to create workflow");
        }

        logger.info("Created workflow with id: {} for user: {}", workflowId, createdBy);

        return new NounMetadata(workflowId, PixelDataType.CONST_INT);
    }
}
