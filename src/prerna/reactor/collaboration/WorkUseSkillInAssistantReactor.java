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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.collaboration.BrainProfileUtils;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkUseSkillInAssistant(skillId=["..."], use=[true]);
public class WorkUseSkillInAssistantReactor extends AbstractCollaborationReactor {

	private static final String SKILL_ID = "skillId";
	private static final String USE = "use";

	public WorkUseSkillInAssistantReactor() {
		this.keysToGet = new String[] { SKILL_ID, USE };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String skillId = getString(SKILL_ID).trim();
		boolean use = !Boolean.FALSE.equals(getBoolean(USE));
		List<String> skills = new ArrayList<>(BrainProfileUtils.assistantSkills(user));
		skills.remove(skillId);
		if (use) {
			skills.add(skillId);
		}
		// the save checks each is one of the owner's own skills
		return mapResult(BrainProfileUtils.saveSettings(user, Map.of("assistantSkills", skills)));
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// changes what every chat of the owner's loads, so it waits for their approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		return USE.equals(key) ? MCP_KEY_TYPE.BOOLEAN : super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return "Turns one of the owner's own skills on or off for their assistant, so their chats load it when "
				+ "relevant. Takes effect from the next message.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_ID.equals(key)) {
			return "Id of the skill, from ListMySkills or PublishSkill.";
		}
		if (USE.equals(key)) {
			return "true to turn it on (default), false to turn it off.";
		}
		return super.getDescriptionForKey(key);
	}
}
