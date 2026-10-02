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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *******************************************************************************/
package prerna.reactor.project;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Verifies that {@link CreateProjectReactor} accepts only CODE / BLOCKS /
 * INSIGHTS project types and rejects WORKSPACE / SKILL / NOTEBOOK with a clear
 * error directing the caller at the type-specific reactor.
 *
 * <p>
 * WORKSPACE and SKILL projects have additional persistence requirements
 * (inference-tracking WORKSPACE row + WORKSPACE_RESOURCE links for workspaces;
 * skill-metadata wiring for skills) that {@code CreateProject} does not
 * perform. Leaving those paths open let callers create half-materialized
 * projects that downstream readers couldn't see — see the AgentPicker bug
 * that motivated this guard.
 */
class CreateProjectReactorTest {

	private CreateProjectReactor reactor;
	private Insight insight;
	private User user;

	@BeforeEach
	void setUp() {
		reactor = new CreateProjectReactor();
		reactor.setNounStore(new NounStore("test"));
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		reactor.keyValue = new HashMap<>();
		when(insight.getUser()).thenReturn(user);
	}

	private void setKeys(String name, String projectType) {
		reactor.keyValue.put(ReactorKeysEnum.PROJECT.getKey(), name);
		if (projectType != null) {
			reactor.keyValue.put(ReactorKeysEnum.PROJECT_TYPE.getKey(), projectType);
		}
		reactor.keyValue.put(ReactorKeysEnum.GLOBAL.getKey(), "false");
	}

	// ---------- rejected types ----------

	@Test
	void rejectsWorkspaceType() {
		setKeys("MyProject", "WORKSPACE");
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
		assertTrue(ex.getMessage().contains("WORKSPACE"),
				"error must name the rejected type — actual: " + ex.getMessage());
		assertTrue(ex.getMessage().contains("AddWorkspace"),
				"error must point caller at AddWorkspace — actual: " + ex.getMessage());
	}

	@Test
	void rejectsSkillType() {
		setKeys("MyProject", "SKILL");
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
		assertTrue(ex.getMessage().contains("SKILL"),
				"error must name the rejected type — actual: " + ex.getMessage());
		assertTrue(ex.getMessage().contains("CreateSkill"),
				"error must point caller at CreateSkill — actual: " + ex.getMessage());
	}

	@Test
	void rejectsNotebookType() {
		setKeys("MyProject", "NOTEBOOK");
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
		assertTrue(ex.getMessage().contains("NOTEBOOK"),
				"error must name the rejected type — actual: " + ex.getMessage());
		assertTrue(ex.getMessage().contains("CreateNotebook"),
				"error must point caller at CreateNotebook — actual: " + ex.getMessage());
	}

	@Test
	void rejectsUnknownProjectTypeWithAllowedListInMessage() {
		setKeys("MyProject", "NOT_A_REAL_TYPE");
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
		assertTrue(ex.getMessage().contains("NOT_A_REAL_TYPE"));
		assertTrue(ex.getMessage().contains("CODE"));
		assertTrue(ex.getMessage().contains("BLOCKS"));
		assertTrue(ex.getMessage().contains("INSIGHTS"));
	}

	@Test
	void rejectsWorkspaceTypeRegardlessOfCasing() {
		// PROJECT_TYPE.valueOf is case-sensitive; lowercase falls through to
		// the unknown-type catch and still rejects. Confirms callers can't
		// sneak WORKSPACE through with a casing trick.
		setKeys("MyProject", "workspace");
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
		assertTrue(ex.getMessage().contains("workspace") || ex.getMessage().contains("Invalid"),
				"error must reject the lowercase variant — actual: " + ex.getMessage());
	}

	// ---------- name validation precedes type checks ----------

	@Test
	void invalidNameStillRejectedBeforeTypeCheck() {
		// Name validation lives upstream of the type check; confirm the
		// bad-name path still triggers and we haven't reordered it.
		setKeys("123 bad name starts with digit", "WORKSPACE");
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> reactor.execute());
		assertTrue(ex.getMessage().contains("Invalid Name"));
	}
}
