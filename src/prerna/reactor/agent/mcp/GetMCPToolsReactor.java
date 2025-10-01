package prerna.reactor.agent.mcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetMCPToolsReactor extends BaseMCPReactor {

	private static final Logger classLogger = LogManager.getLogger(GetMCPToolsReactor.class);

	public GetMCPToolsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
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
		
        if (engine instanceof IMCP) {
            JSONObject tools = ((IMCP) engine).getMCPTools();
    		classLogger.info("Getting MCP Tools for engine {} {}", engineId, tools);
            return new NounMetadata(tools, PixelDataType.JSON_OBJECT);
        }
		
		return new NounMetadata(new JSONObject(), PixelDataType.JSON_OBJECT);
	}

}
