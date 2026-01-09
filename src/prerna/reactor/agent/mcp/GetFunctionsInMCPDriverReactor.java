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
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GetFunctionsInMCPDriverReactor extends AbstractBaseMCPReactor {

	private static final Logger classLogger = LogManager.getLogger(GetFunctionsInMCPDriverReactor.class);

	public GetFunctionsInMCPDriverReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
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
				return new NounMetadata(new ArrayList<>(), PixelDataType.CONST_STRING);
			}
		}

		List<String> functionNames = MCPUtility.getAllFunctionsFromPyFile(this.insight, mcpPyFileLoc);
		return new NounMetadata(functionNames, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "List all the existing functions (at the root level) in the mcp_driver.py file of the engine";
	}

}
