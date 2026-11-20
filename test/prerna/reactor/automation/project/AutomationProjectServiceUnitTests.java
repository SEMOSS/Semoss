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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.CatalogDependencyDerivation;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/** Covers the owner-scoped run scope used by runs of inline definition JSON. */
public class AutomationProjectServiceUnitTests {

	@TempDir
	Path repositoryFolder;

	private static User user(String id) {
		AccessToken token = new AccessToken();
		token.setId(id);
		token.setProvider(AuthProvider.NATIVE);
		User user = new User();
		user.setAccessToken(token);
		return user;
	}

	@Test
	void inlineRunScopeIsStablePerLoginAndDistinctAcrossLogins() {
		String scope = AutomationProjectService.inlineRunScope(user("owner"));

		assertTrue(scope.startsWith(AutomationConstants.INLINE_RUN_SCOPE_PREFIX));
		assertTrue(AutomationProjectService.isInlineRunScope(scope));
		assertEquals(scope, AutomationProjectService.inlineRunScope(user("owner")));
		assertNotEquals(scope, AutomationProjectService.inlineRunScope(user("other")));
		assertFalse(AutomationProjectService.isInlineRunScope("project-id"));
		assertFalse(AutomationProjectService.isInlineRunScope(null));
	}

	@Test
	void blankProjectResolvesToTheCallersOwnScopeWithoutAnAclCall() {
		User owner = user("owner");
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			assertEquals(AutomationProjectService.inlineRunScope(owner),
					AutomationProjectService.resolveRunScope(owner, null, false));
			assertEquals(AutomationProjectService.inlineRunScope(owner),
					AutomationProjectService.resolveRunScope(owner, " ", true));
			security.verifyNoInteractions();
		}
	}

	@Test
	void acceptsOnlyTheCallersOwnInlineScope() {
		User owner = user("owner");
		String ownScope = AutomationProjectService.inlineRunScope(owner);
		String otherScope = AutomationProjectService.inlineRunScope(user("other"));
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			assertEquals(ownScope, AutomationProjectService.resolveRunScope(owner, ownScope, true));
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.resolveRunScope(owner, otherScope, false));
			assertEquals("Run does not exist or user does not have access.", error.getMessage());
			security.verifyNoInteractions();
		}
	}

	@Test
	void projectLookupRejectsInlinePrefixedIds() {
		User owner = user("owner");
		String otherScope = AutomationProjectService.inlineRunScope(user("other"));
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			// Even a project registered under that id must not expose the other user's
			// runs.
			security.when(() -> SecurityProjectUtils.userCanViewProject(any(), anyString())).thenReturn(true);
			security.when(() -> SecurityProjectUtils.userCanEditProject(any(), anyString())).thenReturn(true);

			IllegalArgumentException direct = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.getViewableAutomationProject(owner, otherScope));
			assertEquals("Project does not exist or user does not have access.", direct.getMessage());
			IllegalArgumentException edit = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.getEditableAutomationProject(owner, otherScope));
			assertEquals("Project does not exist or user does not have edit access.", edit.getMessage());
			security.verify(() -> SecurityProjectUtils.userCanViewProject(any(), anyString()), never());
			security.verify(() -> SecurityProjectUtils.userCanEditProject(any(), anyString()), never());
		}
	}

	@Test
	void namedProjectStillUsesTheProjectAcl() {
		User owner = user("owner");
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.resolveRunScope(owner, "project-id", true));
			assertEquals("Project does not exist or user does not have edit access.", error.getMessage());
			security.verify(() -> SecurityProjectUtils.userCanEditProject(owner, "project-id"));
		}
	}

	@Test
	void commitsChangedDefinitionAssetsWithTheRequestedMessage() throws Exception {
		Path versionFolder = repositoryFolder.resolve("version");
		Files.createDirectories(versionFolder);
		String initialHead;
		try (Git git = Git.init().setDirectory(versionFolder.toFile()).call()) {
			Path readme = versionFolder.resolve("README.md");
			Files.writeString(readme, "initial");
			git.add().addFilepattern("README.md").call();
			initialHead = git.commit().setMessage("initial").setAuthor("Tester", "tester@example.com")
					.setCommitter("Tester", "tester@example.com").call().getId().name();
		}

		Path definition = versionFolder.resolve("assets/automation-workflow.json");
		Files.createDirectories(definition.getParent());
		Files.writeString(definition, "{\"formatVersion\":2}");

		AutomationProjectService.commitDefinitionAssets(versionFolder.toString(), List.of(definition.toString()),
				"Restore automation definition from abc123", user("owner"));

		try (Git git = Git.open(versionFolder.toFile())) {
			assertNotEquals(initialHead, git.getRepository().resolve("HEAD").name());
			assertEquals("Restore automation definition from abc123",
					git.log().setMaxCount(1).call().iterator().next().getFullMessage());
		}
	}

	@Test
	void reportsGitFailureInsteadOfSilentlySavingWithoutHistory() throws Exception {
		Path versionFolder = repositoryFolder.resolve("version");
		Files.createDirectories(versionFolder);
		Path definition = versionFolder.resolve("assets/automation-workflow.json");
		Git.init().setBare(true).setDirectory(versionFolder.toFile()).call().close();
		Files.createDirectories(definition.getParent());
		Files.writeString(definition, "{\"formatVersion\":2}");

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationProjectService.commitDefinitionAssets(versionFolder.toString(),
						List.of(definition.toString()), "Update automation definition", user("owner")));

		assertEquals("Unable to commit Automation definition assets to version history.", error.getMessage());
	}

	/**
	 * Every definition mutation enters through saveDefinition, so the dependency
	 * sync belongs there rather than in one reactor: a granular step edit or a
	 * version restore records the same engines a full save does, and the nodes of a
	 * loop body count as much as the steps beside it.
	 */
	@Test
	void saveRecordsTheEnginesOfNestedStepsAsAutomaticDependencies() throws Exception {
		Path versionFolder = repositoryFolder.resolve("automation-version");
		Files.createDirectories(versionFolder);
		Git.init().setDirectory(versionFolder.toFile()).call().close();
		User owner = user("owner");
		String definitionJson = loopDefinition();
		Path definitionFile = versionFolder.resolve("assets/automation-workflow.json");
		Files.createDirectories(definitionFile.getParent());
		Files.writeString(definitionFile, definitionJson);
		AutomationDefinitionService.DefinitionFiles files = new AutomationDefinitionService.DefinitionFiles(
				definitionJson, Map.of());
		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn("Automation");

		try (MockedStatic<AutomationDefinitionService> definitions = mockStatic(AutomationDefinitionService.class);
				MockedStatic<AutomationMcpSync> mcp = mockStatic(AutomationMcpSync.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<AssetUtility> assets = mockStatic(AssetUtility.class);
				MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class);
				MockedStatic<SecurityEngineUtils> engines = mockStatic(SecurityEngineUtils.class);
				MockedStatic<CatalogDependencyDerivation> dependencies = mockStatic(
						CatalogDependencyDerivation.class)) {
			definitions.when(() -> AutomationDefinitionService.load("automation-1")).thenReturn(files);
			definitions.when(() -> AutomationDefinitionService.save("automation-1", definitionJson, Map.of()))
					.thenReturn(files);
			definitions.when(() -> AutomationDefinitionService.getArtifactPaths("automation-1"))
					.thenReturn(List.of(definitionFile));
			utility.when(() -> Utility.getProject("automation-1")).thenReturn(project);
			assets.when(() -> AssetUtility.getProjectAssetsFolder("automation-1"))
					.thenReturn(repositoryFolder.toString());
			assets.when(() -> AssetUtility.getProjectVersionFolder("Automation", "automation-1"))
					.thenReturn(versionFolder.toString());
			engines.when(() -> SecurityEngineUtils.userCanViewEngine(owner, "engine-1")).thenReturn(true);
			engines.when(() -> SecurityEngineUtils.getEngineType("engine-1")).thenReturn(IEngine.CATALOG_TYPE.DATABASE);

			AutomationProjectService.saveDefinition("automation-1", definitionJson, Map.of(), null, owner);

			@SuppressWarnings("unchecked")
			ArgumentCaptor<Collection<Map<String, Object>>> synced = ArgumentCaptor.forClass(Collection.class);
			dependencies.verify(() -> CatalogDependencyDerivation.syncAutomation(eq(owner), eq("automation-1"),
					synced.capture()));
			assertEquals(List.of("start", "work", "body-query"),
					synced.getValue().stream()
							.map(node -> String.valueOf(node.get(AutomationConstants.NODE_FIELD_ID))).toList());
		}
	}

	/** A trigger, a for-each loop, and a database query inside the loop's body. */
	private static String loopDefinition() {
		Map<String, Object> bodyNode = new LinkedHashMap<>();
		bodyNode.put(AutomationConstants.NODE_FIELD_ID, "body-query");
		bodyNode.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DATABASE_QUERY);
		bodyNode.put(AutomationConstants.NODE_FIELD_LABEL, "Read claims");
		bodyNode.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, "claims");
		bodyNode.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		bodyNode.put(AutomationConstants.NODE_FIELD_CONFIG,
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", "query",
						"SELECT * FROM CLAIMS ORDER BY ID", AutomationConstants.CONFIG_LIMIT, 100));

		Map<String, Object> loop = new LinkedHashMap<>();
		loop.put(AutomationConstants.NODE_FIELD_ID, "work");
		loop.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_CONTROL_LOOP);
		loop.put(AutomationConstants.NODE_FIELD_LABEL, "For each claim");
		loop.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, "work_1");
		loop.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		loop.put(AutomationConstants.NODE_FIELD_CONFIG,
				Map.of(AutomationConstants.CONFIG_LOOP_MODE, AutomationConstants.LOOP_MODE_FOR_EACH,
						AutomationConstants.CONFIG_LOOP_ITEMS, List.of(1, 2),
						AutomationConstants.CONFIG_LOOP_BATCH_SIZE, 1,
						AutomationConstants.CONFIG_LOOP_MAX_ITERATIONS, 10));
		loop.put(AutomationConstants.NODE_FIELD_BODY,
				Map.of(AutomationConstants.DOC_NODES, List.of(bodyNode), AutomationConstants.DOC_EDGES, List.of()));

		Map<String, Object> start = new LinkedHashMap<>();
		start.put(AutomationConstants.NODE_FIELD_ID, "start");
		start.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_START);
		start.put(AutomationConstants.NODE_FIELD_LABEL, "Start");
		start.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		start.put(AutomationConstants.NODE_FIELD_CONFIG, Map.of());

		Map<String, Object> edge = Map.of(AutomationConstants.NODE_FIELD_ID, "e-start-work",
				AutomationConstants.EDGE_FIELD_KIND, AutomationConstants.EDGE_KIND_CONTROL,
				AutomationConstants.EDGE_FIELD_SOURCE, "start", AutomationConstants.EDGE_FIELD_SOURCE_PORT,
				AutomationConstants.CONTROL_PORT_OUT, AutomationConstants.EDGE_FIELD_TARGET, "work",
				AutomationConstants.EDGE_FIELD_TARGET_PORT, AutomationConstants.CONTROL_PORT_IN);

		return AutomationRuntimeUtils.GSON.toJson(Map.of(AutomationConstants.DOC_FORMAT_VERSION,
				AutomationConstants.PYTHON_DOC_CURRENT_VERSION, AutomationConstants.DOC_TRIGGER_BINDINGS,
				List.of(Map.of("id", "manual", "type", "manual")), AutomationConstants.DOC_GRAPH,
				Map.of(AutomationConstants.DOC_NODES, List.of(start, loop), AutomationConstants.DOC_EDGES,
						List.of(edge))));
	}

	@Test
	void agentRunWithoutAWorkspaceSkipsOnlyTheWorkspaceCheck() {
		User owner = user("owner");
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class);
				MockedStatic<SecurityEngineUtils> engines = mockStatic(SecurityEngineUtils.class)) {
			engines.when(() -> SecurityEngineUtils.userCanViewEngine(owner, "engine-1")).thenReturn(true);
			engines.when(() -> SecurityEngineUtils.getEngineType("engine-1")).thenReturn(IEngine.CATALOG_TYPE.MODEL);

			AutomationProjectService.validateDefinitionReferences(agentDefinition(null), owner);
			security.verifyNoInteractions();

			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> AutomationProjectService.validateDefinitionReferences(agentDefinition("workspace-1"), owner));
			assertTrue(error.getMessage().contains("workspaceId 'workspace-1'"));
		}
	}

	private static AutomationDefinitionValidator.ValidatedDefinition agentDefinition(String workspaceId) {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put(AutomationConstants.CONFIG_ENGINE_ID, "engine-1");
		config.put(AutomationConstants.CONFIG_COMMAND, "Summarize");
		if (workspaceId != null) {
			config.put(AutomationConstants.CONFIG_WORKSPACE_ID, workspaceId);
		}
		Map<String, Object> start = Map.of(AutomationConstants.NODE_FIELD_ID, "start",
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_START,
				AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED,
				AutomationConstants.NODE_FIELD_CONFIG, Map.of());
		Map<String, Object> agent = Map.of(AutomationConstants.NODE_FIELD_ID, "agent",
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_AGENT_RUN,
				AutomationConstants.NODE_FIELD_OUTPUT_VAR, "brief", AutomationConstants.NODE_FIELD_CODE_MODE,
				AutomationConstants.NODE_CODE_MODE_GENERATED, AutomationConstants.NODE_FIELD_CONFIG, config);
		Map<String, Object> edge = Map.of(AutomationConstants.NODE_FIELD_ID, "e1", AutomationConstants.EDGE_FIELD_KIND,
				AutomationConstants.EDGE_KIND_CONTROL, AutomationConstants.EDGE_FIELD_SOURCE, "start",
				AutomationConstants.EDGE_FIELD_SOURCE_PORT, AutomationConstants.CONTROL_PORT_OUT,
				AutomationConstants.EDGE_FIELD_TARGET, "agent", AutomationConstants.EDGE_FIELD_TARGET_PORT,
				AutomationConstants.CONTROL_PORT_IN);
		return AutomationDefinitionValidator.parseAndValidate(AutomationRuntimeUtils.GSON
				.toJson(Map.of(AutomationConstants.DOC_FORMAT_VERSION, AutomationConstants.PYTHON_DOC_CURRENT_VERSION,
						AutomationConstants.DOC_TRIGGER_BINDINGS, List.of(Map.of("id", "manual", "type", "manual")),
						AutomationConstants.DOC_GRAPH, Map.of(AutomationConstants.DOC_NODES, List.of(start, agent),
								AutomationConstants.DOC_EDGES, List.of(edge)))));
	}

	@Test
	void rejectsUsersWithoutLoginAndMissingUsers() {
		assertThrows(IllegalArgumentException.class, () -> AutomationProjectService.inlineRunScope(new User()));
		assertThrows(IllegalArgumentException.class, () -> AutomationProjectService.inlineRunScope(null));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationProjectService.resolveRunScope(new User(), null, false));
	}
}
