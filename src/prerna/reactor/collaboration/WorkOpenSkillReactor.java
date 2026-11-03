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

import prerna.collaboration.CollaborationSkillFiles;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkOpenSkill(skillId=["..."]);
public class WorkOpenSkillReactor extends AbstractCollaborationReactor {

	private static final String SKILL_ID = "skillId";

	public WorkOpenSkillReactor() {
		this.keysToGet = new String[] { SKILL_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		String skillId = getString(SKILL_ID);
		if (skillId == null || skillId.isBlank()) {
			throw new IllegalArgumentException("Say which skill to open, by its id from ListMySkills");
		}
		return mapResult(CollaborationSkillFiles.open(getUser(), this.insight.getRoomId(), skillId.trim()));
	}

	@Override
	public String getReactorDescription() {
		return "Copies one of the owner's skills into this chat's folder as " + CollaborationSkillFiles.DRAFTS
				+ "/<name>/ so you can read or change it with the file tools, and returns its SKILL.md and file "
				+ "list. Replaces any older copy there. Nothing changes for the owner until PublishSkill.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_ID.equals(key)) {
			return "Id of the owner's skill, from ListMySkills.";
		}
		return super.getDescriptionForKey(key);
	}
}
