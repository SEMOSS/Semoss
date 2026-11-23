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
package prerna.reactor.collaboration;

import java.util.Map;

import prerna.collaboration.WorkOrgChart;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class WorkShowOrgChartReactor extends AbstractCollaborationReactor {

	private static final String QUERY = "query";
	private static final String PERSON_ID = "personId";

	public WorkShowOrgChartReactor() {
		this.keysToGet = new String[] { QUERY, PERSON_ID };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(WorkOrgChart.show(getUser(), getString(QUERY), getString(PERSON_ID)));
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.UI_COMPONENT, MCPUtility.COMPONENT_ORG_CHART);
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.INLINE.getValue());
		meta.put(MCPUtility.UI_LOADING_MESSAGE, "Reading the org chart...");
		return meta;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Show a person's Microsoft 365 org chart, including their manager, direct reports and "
						+ "people who share their manager. Ask the owner to choose when several people match. "
						+ "Use section statuses to distinguish missing directory links from an incomplete lookup.")
				.key(QUERY, "A coworker's name, exact email address, Microsoft sign-in, directory id, or me")
				.key(PERSON_ID, "An exact Microsoft directory user id from a previous chart, in place of query")
				.output(Map.class, "The selected person's chart, matching candidates, or the lookup status");
	}
}
