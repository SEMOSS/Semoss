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

	private final String PROTOCOL_VERSION = "protocolVersion";

	public InitMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() + "," + ReactorKeysEnum.PROJECT.getKey(),
				PROTOCOL_VERSION };
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
