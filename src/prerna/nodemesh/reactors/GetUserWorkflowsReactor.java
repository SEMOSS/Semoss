package prerna.nodemesh.reactors;


import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.auth.User;
import prerna.nodemesh.db.Queries;


public class GetUserWorkflowsReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(GetUserWorkflowsReactor.class);

    public GetUserWorkflowsReactor() {
    }

    @Override
    public NounMetadata execute() {
        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }
        List<Map<String, Object>> userWorkflows = Queries.getUserWorkflows(user.getPrimaryLoginToken().getId());
        return new NounMetadata(userWorkflows, PixelDataType.VECTOR);
    }
}
