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

import prerna.collaboration.CollaborationSkillBuilder;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkEditSkill(skillId=["..."], brief=["Add a column for the meeting organizer"]);
public class WorkEditSkillReactor extends AbstractCollaborationReactor {

	private static final String SKILL_ID = "skillId";
	private static final String BRIEF = "brief";

	public WorkEditSkillReactor() {
		this.keysToGet = new String[] { SKILL_ID, BRIEF };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(CollaborationSkillBuilder.build(getUser(), this.insight, getString(SKILL_ID).trim(),
				getString(BRIEF), false));
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// changes instructions the owner's agents follow, so it waits for their approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Changes one of the owner's skills: a skill builder reads its files and edits, adds or removes "
				+ "SKILL.md, references, HTML templates and scripts as the brief asks. Use it when the owner wants "
				+ "a skill or a routine's result to work or look different. Read the skill with ReadMySkill first "
				+ "when you need to know what is there. It takes a few minutes and returns what changed; each "
				+ "change is kept as a version.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_ID.equals(key)) {
			return "Id of the owner's skill, from ListMySkills.";
		}
		if (BRIEF.equals(key)) {
			return "What to change and why, with everything the builder needs, since it cannot see this chat. At "
					+ "most 8000 characters.";
		}
		return super.getDescriptionForKey(key);
	}
}
