package prerna.reactor.agent.mcp;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
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

	
	public InitMCPReactor()
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
		IProject project = Utility.getProject(keyValue.get(keysToGet[0]));
		
		String projectName = project.getProjectName();
		
		Map resultMap = new HashMap();
		resultMap.put("protocolVersion", "2024-11-05");
		
		Map serverMap = new HashMap();
		serverMap.put("name",projectName);
		serverMap.put("version","1.8.0");
		resultMap.put("serverInfo", serverMap);
		
		
		Map capabilitiesMap = new HashMap();
		capabilitiesMap.put("experimental", new JSONObject());
		
		Map promptMap = new HashMap();
		promptMap.put("listChanged", false);
		capabilitiesMap.put("prompts", promptMap);
		
		Map resourcesMap = new HashMap();
		resourcesMap.put("listChanged", false);
		resourcesMap.put("subscribe", false);
		capabilitiesMap.put("resources", resourcesMap);
		
		Map toolsMap = new HashMap();
		toolsMap.put("listChanged", false);
		capabilitiesMap.put("tools", toolsMap);

		resultMap.put("capabilities", capabilitiesMap);
		
		
		return new NounMetadata(resultMap, PixelDataType.MAP);
	}

}
