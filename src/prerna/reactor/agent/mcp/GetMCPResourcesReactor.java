package prerna.reactor.agent.mcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class GetMCPResourcesReactor extends GetMCPToolsReactor {

	// responsible for making the mcp
	// looks for project id and then makes the MCP based on it
	private static final Logger classLogger = LogManager.getLogger(GetMCPResourcesReactor.class);

	
	public GetMCPResourcesReactor()
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
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(keyValue.get(keysToGet[0]));
		// need to apply the same from java etc. 
		String jsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		JSONArray pyToolArray = getNode(jsonFileLoc, "resources");
		jsonFileLoc = projectAssetFolder + "/mcp/java_mcp.json";
		JSONArray javaToolArray = getNode(jsonFileLoc, "resources");
		pyToolArray.putAll(javaToolArray);
		
		JSONObject toolMap = new JSONObject();
		toolMap.put("resources", pyToolArray);
		return new NounMetadata(toolMap, PixelDataType.JSON_OBJECT);
	}
	
}
