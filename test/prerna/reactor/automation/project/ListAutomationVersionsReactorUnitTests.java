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
package prerna.reactor.automation.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Covers the ACL and input boundary for Automation version listing. */
class ListAutomationVersionsReactorUnitTests {

	private static final String PROJECT_KEY = "project";
	private static final String LIMIT_KEY = "limit";

	private ListAutomationVersionsReactor reactor;
	private Insight insight;
	private User user;

	@BeforeEach
	void setup() {
		reactor = new ListAutomationVersionsReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);
	}

	@Test
	void nullUserThrowsPixelException() {
		when(insight.getUser()).thenReturn(null);
		reactor.keyValue.put(PROJECT_KEY, "project-1");

		assertThrows(SemossPixelException.class, reactor::execute);
	}

	@Test
	void missingProjectThrows() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
		assertEquals("Required input(s) missing: project", error.getMessage());
	}

	@Test
	void accessDeniedPropagatesWhenUserLacksViewPermission() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");

		try (var projectService = mockStatic(AutomationProjectService.class)) {
			projectService.when(() -> AutomationProjectService.getViewableAutomationProject(user, "project-1"))
					.thenThrow(new IllegalArgumentException("Project does not exist or user does not have access."));

			IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Project does not exist or user does not have access.", error.getMessage());
		}
	}

	@Test
	void zeroLimitThrows() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");
		reactor.getNounStore().makeNoun(LIMIT_KEY).addLiteral("0");
		IProject project = project();

		try (var projectService = mockStatic(AutomationProjectService.class)) {
			projectService.when(() -> AutomationProjectService.getViewableAutomationProject(user, "project-1"))
					.thenReturn(project);

			IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("limit must be a positive integer.", error.getMessage());
		}
	}

	@Test
	void returnsVersionsFromAuthorizedProject() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");
		IProject project = project();
		List<Map<String, Object>> versions = List.of(
				Map.of("user", "Alice", "date", "2024-01-02", "message", "Save", "id", "a".repeat(40)));

		try (var projectService = mockStatic(AutomationProjectService.class);
				var versionService = mockStatic(AutomationVersionService.class)) {
			projectService.when(() -> AutomationProjectService.getViewableAutomationProject(user, "project-1"))
					.thenReturn(project);
			versionService.when(() -> AutomationVersionService.listVersions(project, 25)).thenReturn(versions);

			NounMetadata result = reactor.execute();

			assertEquals(versions, result.getValue());
		}
	}

	@Test
	void capsRequestedLimitAt100BeforeReadingHistory() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");
		reactor.getNounStore().makeNoun(LIMIT_KEY).addLiteral("999");
		IProject project = project();

		try (var projectService = mockStatic(AutomationProjectService.class);
				var versionService = mockStatic(AutomationVersionService.class)) {
			projectService.when(() -> AutomationProjectService.getViewableAutomationProject(user, "project-1"))
					.thenReturn(project);
			versionService.when(() -> AutomationVersionService.listVersions(project, 100)).thenReturn(List.of());

			assertEquals(List.of(), reactor.execute().getValue());
			versionService.verify(() -> AutomationVersionService.listVersions(project, 100));
		}
	}

	private static IProject project() {
		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("project-1");
		when(project.getProjectName()).thenReturn("My Project");
		return project;
	}
}
