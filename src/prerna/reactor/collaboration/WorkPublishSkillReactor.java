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

import prerna.collaboration.CollaborationSkillFiles;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkPublishSkill(path=["skills/meeting-digest"]);
public class WorkPublishSkillReactor extends AbstractCollaborationReactor {

	private static final String PATH = "path";

	public WorkPublishSkillReactor() {
		this.keysToGet = new String[] { PATH };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(CollaborationSkillFiles.publish(getUser(), this.insight.getRoomId(), getString(PATH)));
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
		return "Publishes a skill folder you wrote in this chat's folder as one of the owner's private skills: a "
				+ "new skill the first time, else the skill it was opened from with OpenSkill. The skill's files "
				+ "become exactly the folder's. Writing or editing the folder changes nothing for the owner until "
				+ "this runs, so after any change to a skill's files, call it before saying the change is done. "
				+ "Before writing one, call "
				+ "LoadSkill(skill_name=\"collaboration/references/skills/write-a-skill.md\").";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (PATH.equals(key)) {
			return "The folder, such as " + CollaborationSkillFiles.DRAFTS + "/meeting-digest.";
		}
		return super.getDescriptionForKey(key);
	}
}
