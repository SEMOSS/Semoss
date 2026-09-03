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

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Stops a trace-linked child agent run for an Automation project editor. */
public class StopAutomationAgentRunReactor extends AbstractReactor {

	private static final String AUTOMATION_RUN_ID_KEY = "automationRunId";
	private static final String NODE_ID_KEY = "nodeId";
	private static final String AGENT_RUN_ID_KEY = "agentRunId";

	public StopAutomationAgentRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), AUTOMATION_RUN_ID_KEY, NODE_ID_KEY,
				AGENT_RUN_ID_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		AutomationAgentRunAccess.authorizeEdit(this.insight, this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()),
				this.keyValue.get(AUTOMATION_RUN_ID_KEY), this.keyValue.get(NODE_ID_KEY),
				this.keyValue.get(AGENT_RUN_ID_KEY));
		Map<String, Object> run = AgentRuntimeManager.get().stopForAutomation(this.keyValue.get(AGENT_RUN_ID_KEY),
				this.insight);
		run.put("canControl", true);
		return new NounMetadata(run, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Stops a trace-linked Automation child agent run for a project editor.";
	}
}
