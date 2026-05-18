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
package prerna.reactor.agent;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates a skill under {@code <project>/client/.claude/skills/<slug>/SKILL.md}.
 *
 * <p>Skill files live at {@code <project>/client/.claude/skills/<slug>/SKILL.md}.
 * The backing static is on {@link AppBuilderHarnessConfiguration}.
 */
public class CreateAppSkillReactor extends AbstractReactor {

	public CreateAppSkillReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "skillName", "skillContent" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId    = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String skillName    = this.keyValue.get("skillName");
		String skillContent = this.keyValue.get("skillContent");

		User user = this.insight.getUser();
		Boolean response = AppBuilderHarnessConfiguration.createSkill(user, projectId, skillName, skillContent);
		return new NounMetadata(response, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
	}
}
