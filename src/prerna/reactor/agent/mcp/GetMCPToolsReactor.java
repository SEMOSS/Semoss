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
import prerna.tcp.client.NativePySocketClient;
import prerna.util.AssetUtility;

public class GetMCPToolsReactor extends AbstractReactor {

	// responsible for making the mcp
	// looks for project id and then makes the MCP based on it
	private static final Logger classLogger = LogManager.getLogger(GetMCPToolsReactor.class);

	
	public GetMCPToolsReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		organizeKeys();
		JSONObject toolMap = new JSONObject();
		
		// get the project
		// check to see if there is a py directory
		// if there is pick the main.py and ask the system to make the json
		String projectAssetFolder = AssetUtility.getProjectAssetFolder(keyValue.get(keysToGet[0]));
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
	
	protected JSONArray getNode(String jsonFileLoc, String node)
	{
		File jsonFile = new File(jsonFileLoc);
		if(jsonFile.exists())
		{
			InputStream is = null;
			try {
				is = new FileInputStream(jsonFile);
				String jsonTxt = IOUtils.toString(is, "UTF-8");
				JSONObject json = new JSONObject(jsonTxt);
				// the tools is what has it
				JSONArray toolObj = null;
				if(json.has(node))
				{
					toolObj = (JSONArray)json.getJSONArray(node);
					return toolObj;
				}
				is.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return new JSONArray();

	}

}
