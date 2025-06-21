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

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class GetMCPPromptsReactor extends GetMCPToolsReactor {

	// responsible for making the mcp
	// looks for project id and then makes the MCP based on it
	private static final Logger classLogger = LogManager.getLogger(GetMCPPromptsReactor.class);

	
	public GetMCPPromptsReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		organizeKeys();
		
		// get the project
		// check to see if there is a py directory
		// if there is pick the main.py and ask the system to make the json
		String projectAssetFolder = AssetUtility.getProjectAssetFolder(keyValue.get(keysToGet[0]));
		// need to apply the same from java etc. 
		String output = "unprocessed";
		String jsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		JSONArray pyToolArray = getNode(jsonFileLoc, "prompts");
		jsonFileLoc = projectAssetFolder + "/mcp/java_mcp.json";
		JSONArray javaToolArray = getNode(jsonFileLoc, "prompts");
		pyToolArray.putAll(javaToolArray);
		JSONObject toolMap = new JSONObject();
		toolMap.put("prompts", pyToolArray);
		return new NounMetadata(toolMap, PixelDataType.JSON_OBJECT);
	}
	
}
