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
package prerna.reactor.agent.admin;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the direct subagent runs of any user's run, newest first, for the
 * admin Agent Runs page. Admins only.
 */
public class AdminGetSubagentRunsReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";

	public AdminGetSubagentRunsReactor() {
		this.keysToGet = new String[] { RUN_ID_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		AdminAgentRunReadService readService = AdminAgentRunReadService.getInstance(this.insight.getUser());
		organizeKeys();
		String runId = StringUtils.trimToNull(this.keyValue.get(RUN_ID_KEY));
		return new NounMetadata(readService.getSubagentRuns(runId), PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Admins only. Returns the direct subagent runs of any user's agent run, newest first, each with its model's name.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "The parent run whose direct subagent runs are returned.";
		}
		return super.getDescriptionForKey(key);
	}
}
