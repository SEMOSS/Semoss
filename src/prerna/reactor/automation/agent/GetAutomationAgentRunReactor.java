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
package prerna.reactor.automation.agent;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.run.AgentRunService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns a trace-linked child agent run for an Automation project
 * collaborator.
 *
 * <p>
 * Pixel: {@code GetAutomationAgentRun(project=["id"], automationRunId=["id"],
 * nodeId=["id"], agentRunId=["id"], includeMessages=[true])}
 */
public class GetAutomationAgentRunReactor extends AbstractReactor {

	private static final String AUTOMATION_RUN_ID_KEY = "automationRunId";
	private static final String NODE_ID_KEY = "nodeId";
	private static final String AGENT_RUN_ID_KEY = "agentRunId";
	private static final String INCLUDE_MESSAGES_KEY = "includeMessages";

	public GetAutomationAgentRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), AUTOMATION_RUN_ID_KEY, NODE_ID_KEY,
				AGENT_RUN_ID_KEY, INCLUDE_MESSAGES_KEY };
		this.keyRequired = new int[] { 0, 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		AutomationAgentRunAccess.Access access = AutomationAgentRunAccess.authorizeView(this.insight,
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()), this.keyValue.get(AUTOMATION_RUN_ID_KEY),
				this.keyValue.get(NODE_ID_KEY), this.keyValue.get(AGENT_RUN_ID_KEY));
		boolean includeMessages = Boolean.parseBoolean(this.keyValue.get(INCLUDE_MESSAGES_KEY));
		Map<String, Object> run = AgentRunService.get().getRunForAutomation(this.keyValue.get(AGENT_RUN_ID_KEY),
				this.insight, includeMessages);
		run.put("canControl", access.canControl());
		run.put("projectId", access.projectId());
		return new NounMetadata(run, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Returns an exact trace-linked Automation child agent run to project viewers.")
				.key(AGENT_RUN_ID_KEY,
						"The id of the agent run to return. It must be trace-linked to the given Automation run and node.")
				.key(AUTOMATION_RUN_ID_KEY, "The id of the Automation run that started the agent run.")
				.key(INCLUDE_MESSAGES_KEY,
						key -> key.type(Boolean.class).description(
								"Whether to include the agent run's messages in the result. Defaults to false."))
				.key(NODE_ID_KEY, "The id of the Automation node that started the agent run.")
				.key(ReactorKeysEnum.PROJECT,
						"The id or alias of the Automation project the run belongs to; the caller needs view access. Without it, the run must be in the caller's own inline run scope.");
	}
}
