package prerna.reactor.agent.mcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.engine.impl.MCPFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class InitMCPReactor extends AbstractBaseMCPReactor {

	// responsible for making the mcp
	// looks for project id and then makes the MCP based on it

	// expected payload
	// //{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2024-11-05",
	// "capabilities":{"experimental":{},"prompts":{"listChanged":false},
	// "resources":{"subscribe":false,"listChanged":false},
	// "tools":{"listChanged":false}},
	// "serverInfo":{"name":"Stock Price Server","version":"1.8.0"}}}
	private static final Logger classLogger = LogManager.getLogger(InitMCPReactor.class);

	private final String PROTOCOL_VERSION = "protocolVersion";

	public InitMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), PROTOCOL_VERSION };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		IEngine engine = null;
		try {
			engine = Utility.getEngine(engineId);
		} catch (Exception ex) {
			// ignore
		}
		if (engine == null) {
			engine = Utility.getProject(engineId);
		}
		User user = this.insight.getUser();
		checkSecurity(engine, engineId, user);

		String protocolVersion = this.keyValue.get(PROTOCOL_VERSION);

		IMCP mcp = MCPFactory.build(engine);
		return new NounMetadata(mcp.initMCP(protocolVersion), PixelDataType.JSON_OBJECT);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROTOCOL_VERSION)) {
			return "The protocol version that was specified in the Initialization phase from the client";
		}
		return super.getDescriptionForKey(key);
	}

}
