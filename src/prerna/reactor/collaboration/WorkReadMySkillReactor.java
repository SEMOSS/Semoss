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

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.collaboration.CollaborationSkills;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkReadMySkill(skillId=["..."]);  WorkReadMySkill(skillId=["..."], path=["references/format.md"]);
public class WorkReadMySkillReactor extends AbstractCollaborationReactor {

	private static final String SKILL_ID = "skillId";
	private static final String PATH = "path";

	public WorkReadMySkillReactor() {
		this.keysToGet = new String[] { SKILL_ID, PATH };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		String skillId = getString(SKILL_ID).trim();
		String path = getString(PATH);
		if (path == null || path.isBlank()) {
			Map<String, Object> result = new LinkedHashMap<>();
			result.put("skillId", skillId);
			result.put("files", CollaborationSkills.files(getUser(), skillId));
			return mapResult(result);
		}
		return mapResult(CollaborationSkills.read(getUser(), skillId, path));
	}

	@Override
	public String getReactorDescription() {
		return "Lists the files of one of the owner's skills, or reads one of them when a path is given. Use it "
				+ "to show the owner what a skill says or to see what is there before EditSkill.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_ID.equals(key)) {
			return "Id of the owner's skill, from ListMySkills.";
		}
		if (PATH.equals(key)) {
			return "A file path from the list, such as SKILL.md or templates/report.html. Leave out to list the files.";
		}
		return super.getDescriptionForKey(key);
	}
}
