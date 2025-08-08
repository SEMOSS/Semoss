package prerna.reactor.agent.mcp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;

public class GetMCPToolsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetMCPToolsReactor.class);
	
	public GetMCPToolsReactor() {
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
		
		classLogger.info("Getting MCP Tools for project .. " + projectId);
		
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(keyValue.get(keysToGet[0]));
		String pythonJsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		String pixelJsonFileLoc = projectAssetFolder + "/mcp/pixel_mcp.json";
		
		JSONObject toolMap = new JSONObject();
		JSONArray toolsArray = new JSONArray();
		toolsArray.putAll(getNode(pythonJsonFileLoc, "tools"));
		toolsArray.putAll(getNode(pixelJsonFileLoc, "tools"));
		toolMap.put("tools", toolsArray);
		return new NounMetadata(toolMap, PixelDataType.JSON_OBJECT);
	}
	
	/**
	 * 
	 * @param jsonFileLoc
	 * @param node
	 * @return
	 */
	protected JSONArray getNode(String jsonFileLoc, String node) {
		File jsonFile = new File(jsonFileLoc);
		if(jsonFile.exists()) {
			try {
				String jsonTxt = FileUtils.readFileToString(jsonFile, "UTF-8");
				JSONObject json = new JSONObject(jsonTxt);
				if(json.has(node)) {
					JSONArray toolObj = json.getJSONArray(node);
					return toolObj;
				}
			} catch (FileNotFoundException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (JSONException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return new JSONArray();
	}

}
