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

import prerna.collaboration.CollaborationSkills;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkDeleteMySkill(skillId=["..."]);
public class WorkDeleteMySkillReactor extends AbstractCollaborationReactor {

	private static final String SKILL_ID = "skillId";

	public WorkDeleteMySkillReactor() {
		this.keysToGet = new String[] { SKILL_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		String skillId = getString(SKILL_ID).trim();
		CollaborationSkills.delete(getUser(), skillId);
		return mapResult(Map.of("skillId", skillId, "deleted", true));
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// cannot be undone, so it waits for the owner's approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Deletes one of the owner's skills for good. Routines and the assistant stop using it on their next "
				+ "run.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_ID.equals(key)) {
			return "Id of the owner's skill, from ListMySkills.";
		}
		return super.getDescriptionForKey(key);
	}
}
