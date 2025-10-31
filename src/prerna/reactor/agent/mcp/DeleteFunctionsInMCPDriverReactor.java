package prerna.reactor.agent.mcp;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class DeleteFunctionsInMCPDriverReactor extends AbstractBaseMCPReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteFunctionsInMCPDriverReactor.class);

	public DeleteFunctionsInMCPDriverReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "functionName" };
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

		Map<String, Boolean> success = new HashMap<>();

		String assetsDir = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String pyFolderLoc = assetsDir + "/py";
		String mcpPyFileLoc = pyFolderLoc + "/" + MCPUtility.MCP_PY_FILE_NAME;
		File mcpPyFile = new File(mcpPyFileLoc);
		if (!mcpPyFile.exists() || !mcpPyFile.isFile()) {
			// test legacy file name
			mcpPyFileLoc = pyFolderLoc + "/" + MCPUtility.LEGACY_PY_FILE_NAME;
			mcpPyFile = new File(mcpPyFileLoc);
			if (!mcpPyFile.exists() || !mcpPyFile.isFile()) {
				success.put("mcp_driver.py", false);
				success.put("py_mcp.json", false);
				return new NounMetadata(false, PixelDataType.MAP);
			}
		}

		String functionName = this.keyValue.get(this.keysToGet[1]);

		success.put("mcp_driver.py",
				MCPUtility.removeExistingFunctionFromPyFile(this.insight, mcpPyFileLoc, functionName));
		success.put("py_mcp.json", MCPUtility.removePythonFunctionFromMCPJson(engine, functionName));
		return new NounMetadata(success, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Delete an existing function from the mcp_driver.py file and py_mcp.json of the engine";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("functionName")) {
			return "The name of the exisitng function to delete";
		}
		return super.getDescriptionForKey(key);
	}

}
