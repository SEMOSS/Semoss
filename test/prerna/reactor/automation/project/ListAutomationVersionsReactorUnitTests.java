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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

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
			projectService.when(() -> AutomationProjectService.resolveRunScope(user, "project-1", false))
					.thenThrow(new IllegalArgumentException("Project does not exist or user does not have access."));

			IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Project does not exist or user does not have access.", error.getMessage());
		}
	}

	@Test
	void inlineRunScopeIsRejected() {
		String inlineScope = AutomationConstants.INLINE_RUN_SCOPE_PREFIX + "other-user";
		reactor.keyValue.put(PROJECT_KEY, inlineScope);

		try (var projectService = mockStatic(AutomationProjectService.class)) {
			projectService.when(() -> AutomationProjectService.resolveRunScope(user, inlineScope, false))
					.thenThrow(new IllegalArgumentException("Run does not exist or user does not have access."));

			assertThrows(IllegalArgumentException.class, reactor::execute);
		}
	}

	@Test
	void zeroLimitThrows() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");
		reactor.getNounStore().makeNoun(LIMIT_KEY).addLiteral("0");

		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("My Project");

		try (var projectService = mockStatic(AutomationProjectService.class)) {
			projectService.when(() -> AutomationProjectService.resolveRunScope(user, "project-1", false))
					.thenReturn("project-1");

			try (var utility = mockStatic(Utility.class)) {
				utility.when(() -> Utility.getProject("project-1")).thenReturn(project);

				IllegalArgumentException error = assertThrows(IllegalArgumentException.class, reactor::execute);
				assertEquals("limit must be a positive integer.", error.getMessage());
			}
		}
	}

	@Test
	void returnsCommitsFromGit() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");

		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("My Project");

		List<Map<String, Object>> fakeCommits = List.of(
				Map.of("user", "Alice", "date", "2024-01-02", "message", "second save", "id", "abc123"),
				Map.of("user", "Bob", "date", "2024-01-01", "message", "first save", "id", "def456"));

		try (var projectService = mockStatic(AutomationProjectService.class);
				var utility = mockStatic(Utility.class);
				var assetUtility = mockStatic(AssetUtility.class);
				var gitRepo = mockStatic(GitRepoUtils.class)) {

			projectService.when(() -> AutomationProjectService.resolveRunScope(user, "project-1", false))
					.thenReturn("project-1");
			utility.when(() -> Utility.getProject("project-1")).thenReturn(project);
			assetUtility.when(() -> AssetUtility.getProjectVersionFolder("My Project", "project-1"))
					.thenReturn("/fake/version/folder");
			gitRepo.when(() -> GitRepoUtils.getCommits("/fake/version/folder", null))
					.thenReturn(new ArrayList<>(fakeCommits));

			NounMetadata result = reactor.execute();

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> commits = (List<Map<String, Object>>) result.getValue();
			assertEquals(2, commits.size());
			assertEquals("Alice", commits.get(0).get("user"));
			assertEquals("Bob", commits.get(1).get("user"));
		}
	}

	@Test
	void limitCapsBeyondMaximumTo100() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");
		reactor.getNounStore().makeNoun(LIMIT_KEY).addLiteral("999");

		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("My Project");

		// Build 150 fake commits to confirm only 100 are returned
		List<Map<String, Object>> fakeCommits = new ArrayList<>();
		for (int i = 0; i < 150; i++) {
			fakeCommits.add(Map.of("user", "user-" + i, "date", "2024-01-01", "message", "save " + i, "id", "id" + i));
		}

		try (var projectService = mockStatic(AutomationProjectService.class);
				var utility = mockStatic(Utility.class);
				var assetUtility = mockStatic(AssetUtility.class);
				var gitRepo = mockStatic(GitRepoUtils.class)) {

			projectService.when(() -> AutomationProjectService.resolveRunScope(user, "project-1", false))
					.thenReturn("project-1");
			utility.when(() -> Utility.getProject("project-1")).thenReturn(project);
			assetUtility.when(() -> AssetUtility.getProjectVersionFolder("My Project", "project-1"))
					.thenReturn("/fake/version/folder");
			gitRepo.when(() -> GitRepoUtils.getCommits("/fake/version/folder", null))
					.thenReturn(fakeCommits);

			NounMetadata result = reactor.execute();

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> commits = (List<Map<String, Object>>) result.getValue();
			assertEquals(100, commits.size());
		}
	}

	@Test
	void defaultLimitIs25WhenNotProvided() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");

		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("My Project");

		List<Map<String, Object>> fakeCommits = new ArrayList<>();
		for (int i = 0; i < 50; i++) {
			fakeCommits.add(Map.of("user", "user-" + i, "date", "2024-01-01", "message", "save " + i, "id", "id" + i));
		}

		try (var projectService = mockStatic(AutomationProjectService.class);
				var utility = mockStatic(Utility.class);
				var assetUtility = mockStatic(AssetUtility.class);
				var gitRepo = mockStatic(GitRepoUtils.class)) {

			projectService.when(() -> AutomationProjectService.resolveRunScope(user, "project-1", false))
					.thenReturn("project-1");
			utility.when(() -> Utility.getProject("project-1")).thenReturn(project);
			assetUtility.when(() -> AssetUtility.getProjectVersionFolder("My Project", "project-1"))
					.thenReturn("/fake/version/folder");
			gitRepo.when(() -> GitRepoUtils.getCommits("/fake/version/folder", null))
					.thenReturn(fakeCommits);

			NounMetadata result = reactor.execute();

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> commits = (List<Map<String, Object>>) result.getValue();
			assertEquals(25, commits.size());
		}
	}

	@Test
	void emptyGitHistoryReturnsEmptyList() {
		reactor.keyValue.put(PROJECT_KEY, "project-1");

		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("My Project");

		try (var projectService = mockStatic(AutomationProjectService.class);
				var utility = mockStatic(Utility.class);
				var assetUtility = mockStatic(AssetUtility.class);
				var gitRepo = mockStatic(GitRepoUtils.class)) {

			projectService.when(() -> AutomationProjectService.resolveRunScope(user, "project-1", false))
					.thenReturn("project-1");
			utility.when(() -> Utility.getProject("project-1")).thenReturn(project);
			assetUtility.when(() -> AssetUtility.getProjectVersionFolder("My Project", "project-1"))
					.thenReturn("/fake/version/folder");
			gitRepo.when(() -> GitRepoUtils.getCommits("/fake/version/folder", null))
					.thenReturn(new ArrayList<>());

			NounMetadata result = reactor.execute();

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> commits = (List<Map<String, Object>>) result.getValue();
			assertEquals(0, commits.size());
		}
	}
}
