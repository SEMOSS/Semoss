package prerna.reactor.agent.mcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class GetMCPResourcesTemplatesReactor extends GetMCPToolsReactor {

	private static final Logger classLogger = LogManager.getLogger(GetMCPResourcesTemplatesReactor.class);

	public GetMCPResourcesTemplatesReactor() {
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
		
		// get the project
		// check to see if there is a py directory
		// if there is pick the main.py and ask the system to make the json
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(keyValue.get(keysToGet[0]));
		// need to apply the same from java etc. 
		String jsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		JSONArray pyToolArray = MCPUtility.getNode(jsonFileLoc, "resourceTemplates");
		jsonFileLoc = projectAssetFolder + "/mcp/java_mcp.json";
		JSONArray javaToolArray = MCPUtility.getNode(jsonFileLoc, "resourceTemplates");
		pyToolArray.putAll(javaToolArray);
		
		JSONObject toolMap = new JSONObject();
		toolMap.put("resourceTemplates", pyToolArray);
		return new NounMetadata(toolMap, PixelDataType.JSON_OBJECT);
	}
	
}
