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
package prerna.reactor.automation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.AgentToolDecisionHandler;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lets an Automation project editor resolve a pending trace-linked agent action.
 */
public class ResolveAutomationAgentRunActionReactor extends AbstractReactor {

	private static final String AUTOMATION_RUN_ID_KEY = "automationRunId";
	private static final String NODE_ID_KEY = "nodeId";
	private static final String AGENT_RUN_ID_KEY = "agentRunId";
	private static final String ACTION_ID_KEY = "actionId";
	private static final String DECISION_KEY = "decision";

	public ResolveAutomationAgentRunActionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), AUTOMATION_RUN_ID_KEY, NODE_ID_KEY,
				AGENT_RUN_ID_KEY, ACTION_ID_KEY, DECISION_KEY, ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.MCP_TOOL_RESULT.getKey(), ReactorKeysEnum.MCP_TOOL_STATUS.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		AutomationAgentRunAccess.authorizeEdit(this.insight, this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()),
				this.keyValue.get(AUTOMATION_RUN_ID_KEY), this.keyValue.get(NODE_ID_KEY),
				this.keyValue.get(AGENT_RUN_ID_KEY));
		String result = new AgentToolDecisionHandler(this.insight).handleAutomationDecision(
				this.keyValue.get(ACTION_ID_KEY), this.keyValue.get(AGENT_RUN_ID_KEY),
				this.keyValue.get(DECISION_KEY), this.keyValue.get(ReactorKeysEnum.MCP_TOOL_RESULT.getKey()),
				this.keyValue.get(ReactorKeysEnum.MCP_TOOL_STATUS.getKey()), getMap());
		Map<String, Object> output = new HashMap<>();
		output.put("result", result);
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.MCP_TOOL_EXECUTION);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		return mapInputs != null && !mapInputs.isEmpty() ? (Map<String, Object>) mapInputs.get(0).getValue() : null;
	}

	@Override
	public String getReactorDescription() {
		return "Lets an Automation project editor resolve a trace-linked pending agent action.";
	}
}
