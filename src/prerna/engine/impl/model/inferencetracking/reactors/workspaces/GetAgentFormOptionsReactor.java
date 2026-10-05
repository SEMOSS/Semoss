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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.hooks.AgentHookRegistry;
import prerna.reactor.agent.runtime.PlatformAgentTools;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the deployment-level options an agent form needs before an agent
 * exists: the built-in tool catalog and the hook kinds the server recognizes.
 * These are the same {@code default_tools} and {@code known_hook_kinds} that
 * GetWorkspace returns for an existing agent.
 */
public class GetAgentFormOptionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAgentFormOptionsReactor.class);

	public GetAgentFormOptionsReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> options = new HashMap<>();
		try {
			options.put("default_tools", PlatformAgentTools.getDefaultToolDefinitions());
		} catch (Exception e) {
			classLogger.warn("Failed to resolve default agent tools: {}", e.getMessage());
			options.put("default_tools", new ArrayList<>());
		}
		options.put("known_hook_kinds", new ArrayList<>(AgentHookRegistry.knownKinds()));
		options.put("hook_capabilities", AgentHookRegistry.formCapabilities());
		return new NounMetadata(options, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Get the built-in agent tool catalog and hook configuration capabilities for creating an agent";
	}
}
