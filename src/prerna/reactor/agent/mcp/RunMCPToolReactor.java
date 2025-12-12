package prerna.reactor.agent.mcp;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.engine.impl.MCPFactory;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunMCPToolReactor extends AbstractBaseMCPReactor {

	private static final Logger classLogger = LogManager.getLogger(RunMCPToolReactor.class);

	// we should possibly remove the function and param values map
	public RunMCPToolReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FUNCTION.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() , ReactorKeysEnum.MCP_TOOL_RESULT.getKey() };
		this.keyRequired = new int[] { 0, 1, 1 , 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		//the MCP may just be returning the fully executed tool. Return that to the playground. Otherwise execute the tool
		//TODO: this logic should be shifted such that the MCP FE app directly calls AddPlaygroundToolExecution
		String toolExecutionResult = this.keyValue.get(this.keysToGet[3]);
		if(toolExecutionResult != null && !toolExecutionResult.trim().isEmpty()) {
			return  new NounMetadata(toolExecutionResult, PixelDataType.CONST_STRING,
					PixelOperationType.MCP_TOOL_EXECUTION);
		}

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null || engineId.isEmpty()) {
			engineId = insight.getContextProjectId();
			if (engineId == null || engineId.isEmpty()) {
				engineId = insight.getProjectId();
			}
		}
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the project id or set the app context");
		}
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

		String toolName = this.keyValue.get(this.keysToGet[1]);
		if (toolName == null || (toolName = toolName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Tool name must be passed in to execute the mcp tool");
		}
		toolName = MCPUtility.removeEngineIdFromToolsMethodName(engine.getEngineId(), toolName);

		// these are the params
		Map<String, Object> paramMap = getMap();

		IMCP mcp = MCPFactory.build(engine);
		return new NounMetadata(mcp.callTool(toolName, paramMap, this.insight), PixelDataType.CONST_STRING,
				PixelOperationType.MCP_TOOL_EXECUTION);
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Execute a tool defined in the app";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.FUNCTION.getKey())) {
			return "The name of the tool/function to execute";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "A key-value pair map containing the parameter inputs for the tool/function";
		} else if (key.equals(ReactorKeysEnum.MCP_TOOL_RESULT.getKey())) {
			return "If this key is present, its value will be returned as the result of the MCP tool execution, and the tool will not be executed.";
		}
		return super.getDescriptionForKey(key);
	}

}
