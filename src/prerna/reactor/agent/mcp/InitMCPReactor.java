package prerna.reactor.agent.mcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class InitMCPReactor extends BaseMCPReactor {

	// responsible for making the mcp
	// looks for project id and then makes the MCP based on it
	
	// expected payload
	//	//{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2024-11-05",
	//"capabilities":{"experimental":{},"prompts":{"listChanged":false},
	//"resources":{"subscribe":false,"listChanged":false},
	//"tools":{"listChanged":false}},
	//"serverInfo":{"name":"Stock Price Server","version":"1.8.0"}}}
	private static final Logger classLogger = LogManager.getLogger(InitMCPReactor.class);

	private final String PROTOCOL_VERSION = "protocolVersion";
	
	public InitMCPReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), PROTOCOL_VERSION, ReactorKeysEnum.MESSAGE.getKey()};
		this.keyRequired = new int[] {1, 1,0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		String rawMessage = null;
		
		if(this.keyValue.containsKey(keysToGet[2]))
			rawMessage = this.keyValue.get(keysToGet[2]);

		IEngine engine = null;
		try
		{
			engine = Utility.getEngine(engineId);
		}catch(IllegalArgumentException ex)
		{
			engine = Utility.getProject(engineId);
		}
		User user = this.insight.getUser();
		checkSecurity(engine, engineId, user);

		IProject project = Utility.getProject(keyValue.get(keysToGet[0]));
		String projectName = project.getProjectName();
		
		// need to return the protocol version of the client request
		// as part of initialization 
		String protocolVersion = this.keyValue.get(PROTOCOL_VERSION);

		JSONObject resultJson = new JSONObject();
		
		if (engine instanceof IMCP)
            resultJson = ((IMCP) engine).initMCP(protocolVersion, rawMessage);

            return new NounMetadata(resultJson, PixelDataType.JSON_OBJECT);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(PROTOCOL_VERSION)) {
			return "The protocol version that was specified in the Initialization phase from the client"; 
		}
		return super.getDescriptionForKey(key);
	}
	
}
