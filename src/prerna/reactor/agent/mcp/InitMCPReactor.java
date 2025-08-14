package prerna.reactor.agent.mcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class InitMCPReactor extends AbstractReactor {

	// responsible for making the mcp
	// looks for project id and then makes the MCP based on it
	
	// expected payload
	//	//{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2024-11-05",
	//"capabilities":{"experimental":{},"prompts":{"listChanged":false},
	//"resources":{"subscribe":false,"listChanged":false},
	//"tools":{"listChanged":false}},
	//"serverInfo":{"name":"Stock Price Server","version":"1.8.0"}}}
	private static final Logger classLogger = LogManager.getLogger(InitMCPReactor.class);

	public InitMCPReactor() {
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
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have access to edit.");
		}
		
		// get the project
		// check to see if there is a py directory
		// if there is pick the main.py and ask the system to make the json
		IProject project = Utility.getProject(keyValue.get(keysToGet[0]));
		String projectName = project.getProjectName();
		
		JSONObject resultJson = new JSONObject();
		resultJson.put("protocolVersion", "2025-06-18");
		
		JSONObject serverJson = new JSONObject();
		serverJson.put("name", projectName);
		serverJson.put("version", "1.8.0");
		resultJson.put("serverInfo", serverJson);
		
		JSONObject capabilitiesJson = new JSONObject();
		capabilitiesJson.put("experimental", new JSONObject());
		
		JSONObject promptJson = new JSONObject();
		promptJson.put("listChanged", false);
		capabilitiesJson.put("prompts", promptJson);
		
		JSONObject resourcesJson = new JSONObject();
		resourcesJson.put("listChanged", false);
		resourcesJson.put("subscribe", false);
		capabilitiesJson.put("resources", resourcesJson);
		
		JSONObject toolsJson = new JSONObject();
		toolsJson.put("listChanged", false);
		capabilitiesJson.put("tools", toolsJson);

		resultJson.put("capabilities", capabilitiesJson);
		return new NounMetadata(resultJson, PixelDataType.JSON_OBJECT);
	}

}
