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

import prerna.auth.User;
import prerna.collaboration.CollaborationSkillBuilder;
import prerna.collaboration.CollaborationSkills;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkCreateSkill(name=["client-summaries"], description=["..."], brief=["..."]);
public class WorkCreateSkillReactor extends AbstractCollaborationReactor {

	private static final String NAME = "name";
	private static final String DESCRIPTION = "description";
	private static final String BRIEF = "brief";

	public WorkCreateSkillReactor() {
		this.keysToGet = new String[] { NAME, DESCRIPTION, BRIEF };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		Map<String, Object> created = CollaborationSkills.create(user, getString(NAME), getString(DESCRIPTION));
		String skillId = String.valueOf(created.get("id"));
		try {
			return mapResult(CollaborationSkillBuilder.build(user, this.insight, skillId, getString(BRIEF), true));
		} catch (RuntimeException e) {
			// the skill exists with its placeholder; say so, so the next step is EditSkill rather than a duplicate
			throw new IllegalStateException("The skill " + created.get("name") + " (" + skillId + ") was created but "
					+ "building its files failed: " + e.getMessage() + " Use EditSkill with this skillId to retry.", e);
		}
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// saves instructions the owner's agents will follow, so it waits for their approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Creates a skill for the owner and has a skill builder write it: SKILL.md plus any references, HTML "
				+ "templates and Python scripts it needs, the same as building a skill in the platform's workbench. "
				+ "Use it when the owner asks to keep a way of doing something, or a routine's result has a fixed "
				+ "shape they read again and again. The skill is private to the owner. It takes a few minutes and "
				+ "returns the files it wrote. Then attach it with CreateRoutine (skills) or UseSkillInAssistant. "
				+ "To change a skill later use EditSkill, never a second CreateSkill. Load "
				+ "collaboration/references/routines/schedule-a-routine.md first when the skill is for a routine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (NAME.equals(key)) {
			return "Short lowercase name with hyphens, such as client-summaries. It cannot change later.";
		}
		if (DESCRIPTION.equals(key)) {
			return "One or two sentences on when an agent should load the skill.";
		}
		if (BRIEF.equals(key)) {
			return "Everything the builder needs, since it cannot see this chat: what the skill is for, the rules "
					+ "and preferences the owner gave, what the result contains, its sections and order, what to "
					+ "write when there is nothing, and whether it needs an HTML template (describe its columns or "
					+ "sections) or a script. At most 8000 characters.";
		}
		return super.getDescriptionForKey(key);
	}
}
