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
package prerna.reactor.agent.skill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;

/**
 * Which attached skills a run stages: only one the person can view or the agent
 * was given, checked before the skill is loaded.
 */
class SkillStagerUnitTests {

	@TempDir
	Path workingDir;

	@Test
	void aSkillThePersonCannotViewAndTheAgentWasNotGivenIsLeftOutWithoutBeingLoaded() {
		User person = mock(User.class);
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class);
				MockedStatic<SkillProjects> skills = mockStatic(SkillProjects.class)) {
			security.when(() -> SecurityProjectUtils.userCanViewProject(person, "privateskill")).thenReturn(false);

			List<SkillStager.SkillNotLoaded> notLoaded = SkillStager.stage(workingDir.toString(),
					List.of(Map.of("skill_id", "privateskill")), person);

			assertEquals(List.of(new SkillStager.SkillNotLoaded("privateskill", SkillStager.NO_ACCESS)), notLoaded);
			skills.verifyNoInteractions();
		}
	}

	@Test
	void withNoPersonOnlyTheAgentsGrantCouldLetASkillIn() {
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class);
				MockedStatic<SkillProjects> skills = mockStatic(SkillProjects.class)) {
			List<SkillStager.SkillNotLoaded> notLoaded = SkillStager.stage(workingDir.toString(),
					List.of(Map.of("skill_id", "anyskill")), null);

			assertEquals(List.of(new SkillStager.SkillNotLoaded("anyskill", SkillStager.NO_ACCESS)), notLoaded);
			security.verifyNoInteractions();
			skills.verifyNoInteractions();
		}
	}

}
