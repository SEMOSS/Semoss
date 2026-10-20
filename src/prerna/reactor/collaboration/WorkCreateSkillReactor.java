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

// WorkCreateSkill(name=["client-summaries"], description=["..."], content=["..."]);
public class WorkCreateSkillReactor extends AbstractCollaborationReactor {

	private static final String NAME = "name";
	private static final String DESCRIPTION = "description";
	private static final String CONTENT = "content";

	public WorkCreateSkillReactor() {
		this.keysToGet = new String[] { NAME, DESCRIPTION, CONTENT };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(CollaborationSkills.create(getUser(), getString(NAME), getString(DESCRIPTION),
				getString(CONTENT)));
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
		return "Saves a skill for the owner: reusable instructions (how they like a summary written, what to check "
				+ "for a client, a report layout) that their routines and, if they turn it on, their assistant load "
				+ "when relevant. The skill is private to the owner. Use it when the owner asks to remember a way of "
				+ "doing something as a skill, or a routine needs longer standing instructions. Then attach it with "
				+ "CreateRoutine (skills) or UseSkillInAssistant.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (NAME.equals(key)) {
			return "Short lowercase name with hyphens, such as client-summaries.";
		}
		if (DESCRIPTION.equals(key)) {
			return "One sentence on when to use the skill; agents read it to decide whether to load it.";
		}
		if (CONTENT.equals(key)) {
			return "The skill's instructions in Markdown, without frontmatter. At most 20000 characters.";
		}
		return super.getDescriptionForKey(key);
	}
}
