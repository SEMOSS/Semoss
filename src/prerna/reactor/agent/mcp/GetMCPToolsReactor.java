package prerna.reactor.agent.mcp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
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
		
		JSONObject toolMap = new JSONObject();
		
		// get the project
		// check to see if there is a py directory
		// if there is pick the main.py and ask the system to make the json
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(keyValue.get(keysToGet[0]));
		// need to apply the same from java etc. 
		classLogger.info("Getting MCP for project .. " + keyValue.get(keysToGet[0]));
		String jsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		File jsonFile = new File(jsonFileLoc);
		if(jsonFile.exists())
		{
			JSONArray pyToolArray = getNode(jsonFileLoc, "tools");
			jsonFileLoc = projectAssetFolder + "/mcp/java_mcp.json";
			JSONArray javaToolArray = getNode(jsonFileLoc, "tools");
			pyToolArray.putAll(javaToolArray);
			
			// other routines to assimilate other things

			toolMap.put("tools", pyToolArray);
			classLogger.info("Toolsets.. " + toolMap);
		}
		else
		{
			JSONArray empty = new JSONArray();
			toolMap.put("tools", empty);
		}
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
		if(jsonFile.exists())
		{
			try (InputStream is = new FileInputStream(jsonFile);){
				String jsonTxt = IOUtils.toString(is, "UTF-8");
				JSONObject json = new JSONObject(jsonTxt);
				// the tools is what has it
				JSONArray toolObj = null;
				if(json.has(node)) {
					toolObj = (JSONArray)json.getJSONArray(node);
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
