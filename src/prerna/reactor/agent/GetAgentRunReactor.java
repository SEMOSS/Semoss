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
package prerna.reactor.agent;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetAgentRunReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";
	private static final String INCLUDE_MESSAGES_KEY = "includeMessages";

	public GetAgentRunReactor() {
		this.keysToGet = new String[] { RUN_ID_KEY, INCLUDE_MESSAGES_KEY };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String runId = StringUtils.trimToNull(this.keyValue.get(RUN_ID_KEY));
		boolean includeMessages = Boolean.parseBoolean(this.keyValue.get(INCLUDE_MESSAGES_KEY));
		Map<String, Object> result = AgentRuntimeManager.get().getRun(runId, this.insight, includeMessages);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Get durable AgentRun status, output, message ids, error, and pending actions. Pass includeMessages=true to include this run's room messages.";
	}
}
