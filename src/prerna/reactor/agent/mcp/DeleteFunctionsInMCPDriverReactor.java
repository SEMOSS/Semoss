/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.agent.mcp;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class DeleteFunctionsInMCPDriverReactor extends AbstractReactor {

	public DeleteFunctionsInMCPDriverReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() + "," + ReactorKeysEnum.PROJECT.getKey(),
				"functionName" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0].split(",")[0]);
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
		checkEngineEditSecurity(engine, user);

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
		MCPToolDiscoveryService.invalidate(engineId);
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
