package prerna.reactor.agent.mcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetMCPInternalToolsReactor extends GetMCPToolsReactor {

	private static final Logger classLogger = LogManager.getLogger(GetMCPInternalToolsReactor.class);
	
	public GetMCPInternalToolsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have access");
		}

		classLogger.info("Getting Internal MCP Tools for project .. " + projectId);
		
		IProject project = Utility.getProject(projectId);
		JSONObject toolMap = MCPUtility.getAggregatedTools(project);
		JSONObject updatedToolMap = MCPUtility.appendProjectIdToTools(projectId, toolMap);
		return new NounMetadata(updatedToolMap, PixelDataType.JSON_OBJECT);
	}
	
}
