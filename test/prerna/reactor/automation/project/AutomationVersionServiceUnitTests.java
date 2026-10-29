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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.util.AssetUtility;

/** Covers project-Git filtering and exact historical aggregate restoration. */
class AutomationVersionServiceUnitTests {

	private static final String START_ONLY = """
			{
			  "formatVersion": 2,
			  "triggerBindings": [{ "id": "manual", "type": "manual" }],
			  "graph": {
			    "nodes": [{ "id": "start", "type": "trigger.start", "label": "Start",
			      "codeMode": "generated", "config": {} }],
			    "edges": []
			  }
			}
			""";
	private static final String RESTORE_COMMIT_PREFIX = "Restore automation definition from ";

	@TempDir
	Path repositoryFolder;

	@Test
	void listsOnlyAutomationCommitsWithFullIdentifiersAndHonorsLimit() throws Exception {
		try (Git git = Git.init().setDirectory(repositoryFolder.toFile()).call()) {
			commit(git, "README.md", "one", "unrelated");
			RevCommit firstAutomation = commit(git, workflowPath(), START_ONLY, "first automation");
			commit(git, "README.md", "two", "another unrelated");
			RevCommit secondAutomation = commit(git, nodePath(), "def run(scope):\n    return 1\n", "source save");

			try (var assets = mockStatic(AssetUtility.class)) {
				IProject project = project();
				assets.when(() -> AssetUtility.getProjectVersionFolder("Project", "project-1"))
						.thenReturn(repositoryFolder.toString());

				List<Map<String, Object>> versions = AutomationVersionService.listVersions(project, 2);

				assertEquals(Set.of(secondAutomation.getId().name(), firstAutomation.getId().name()),
						Set.copyOf(versions.stream().map(version -> (String) version.get("id")).toList()));
				assertTrue(versions.stream().allMatch(version -> ((String) version.get("id")).length() == 40));
				assertTrue(versions.stream().allMatch(version -> !((String) version.get("date")).isBlank()));
				assertFalse(versions.stream().anyMatch(version -> ((String) version.get("date")).contains("1970")));
			}
		}
	}

	@Test
	void restoresGraphAndDeveloperSourceUsingExpectedCurrentRevision() throws Exception {
		String developerDefinition = developerDefinition();
		try (Git git = Git.init().setDirectory(repositoryFolder.toFile()).call()) {
			commit(git, workflowPath(), developerDefinition, "graph");
			RevCommit target = commit(git, nodePath(), "def run(scope):\n    return 'old'\n", "source");
			commit(git, nodePath(), "def run(scope):\n    return 'new'\n", "later source");

			IProject project = project();
			User user = mock(User.class);
			AutomationDefinitionService.DefinitionFiles saved = new AutomationDefinitionService.DefinitionFiles(
					developerDefinition, Map.of("custom", "def run(scope):\n    return 'old'\n"));
			try (var assets = mockStatic(AssetUtility.class);
					var projects = mockStatic(AutomationProjectService.class)) {
				assets.when(() -> AssetUtility.getProjectVersionFolder("Project", "project-1"))
						.thenReturn(repositoryFolder.toString());
				projects.when(() -> AutomationProjectService.saveDefinition("project-1", developerDefinition,
						Map.of("custom", "def run(scope):\n    return 'old'\n"), "current", user,
						RESTORE_COMMIT_PREFIX + target.getId().name())).thenReturn(saved);

				assertEquals(saved, AutomationVersionService.restore(project, target.getId().name(), "current", user));
				projects.verify(() -> AutomationProjectService.saveDefinition("project-1", developerDefinition,
						Map.of("custom", "def run(scope):\n    return 'old'\n"), "current", user,
						RESTORE_COMMIT_PREFIX + target.getId().name()));
			}
		}
	}

	@Test
	void missingDeveloperSourceFailsBeforeSaving() throws Exception {
		try (Git git = Git.init().setDirectory(repositoryFolder.toFile()).call()) {
			RevCommit target = commit(git, workflowPath(), developerDefinition(), "missing source");
			IProject project = project();
			User user = mock(User.class);
			try (var assets = mockStatic(AssetUtility.class);
					var projects = mockStatic(AutomationProjectService.class)) {
				assets.when(() -> AssetUtility.getProjectVersionFolder("Project", "project-1"))
						.thenReturn(repositoryFolder.toString());

				IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
						() -> AutomationVersionService.restore(project, target.getId().name(), "current", user));

				assertTrue(error.getMessage().contains("Developer source"));
				projects.verifyNoInteractions();
			}
		}
	}

	@Test
	void rejectsReachableCommitThatDidNotChangeAutomation() throws Exception {
		try (Git git = Git.init().setDirectory(repositoryFolder.toFile()).call()) {
			commit(git, workflowPath(), START_ONLY, "automation");
			RevCommit unrelated = commit(git, "README.md", "notes", "unrelated");
			IProject project = project();
			User user = mock(User.class);
			try (var assets = mockStatic(AssetUtility.class);
					var projects = mockStatic(AutomationProjectService.class)) {
				assets.when(() -> AssetUtility.getProjectVersionFolder("Project", "project-1"))
						.thenReturn(repositoryFolder.toString());

				IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
						() -> AutomationVersionService.restore(project, unrelated.getId().name(), "current", user));

				assertEquals("Revision is not an Automation definition version.", error.getMessage());
				projects.verifyNoInteractions();
			}
		}
	}

	private RevCommit commit(Git git, String relativePath, String value, String message) throws Exception {
		Path file = repositoryFolder.resolve(relativePath);
		Files.createDirectories(file.getParent());
		Files.writeString(file, value);
		git.add().addFilepattern(relativePath).call();
		return git.commit().setMessage(message).setAuthor("Tester", "tester@example.com")
				.setCommitter("Tester", "tester@example.com").call();
	}

	private static String workflowPath() {
		return "assets/" + AutomationConstants.AUTOMATION_PYTHON_DEFINITION_FILE_NAME;
	}

	private static String nodePath() {
		Map<String, Object> node = Map.of(AutomationConstants.NODE_FIELD_ID, "custom",
				AutomationConstants.NODE_FIELD_LABEL, "Custom", AutomationConstants.NODE_FIELD_TYPE,
				AutomationConstants.NODE_DEVELOPER_PYTHON);
		return "assets/" + AutomationConstants.AUTOMATION_NODE_SOURCES_FOLDER_NAME + "/"
				+ AutomationDefinitionService.safeNodeFileName(node) + ".py";
	}

	private static IProject project() {
		IProject project = mock(IProject.class);
		when(project.getProjectId()).thenReturn("project-1");
		when(project.getProjectName()).thenReturn("Project");
		return project;
	}

	private static String developerDefinition() {
		return """
				{
				  "formatVersion": 2,
				  "triggerBindings": [{ "id": "manual", "type": "manual" }],
				  "graph": {
				    "nodes": [
				      { "id": "start", "type": "trigger.start", "label": "Start",
				        "codeMode": "generated", "config": {} },
				      { "id": "custom", "type": "developer.python", "label": "Custom", "outputVar": "result",
				        "codeMode": "custom", "config": {} }
				    ],
				    "edges": [{ "id": "e1", "kind": "control", "source": "start", "sourcePort": "out",
				      "target": "custom", "targetPort": "in" }]
				  }
				}
				""";
	}
}
