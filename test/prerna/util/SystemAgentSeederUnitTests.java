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
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.agent.config.AgentConfigLoader;

class SystemAgentSeederUnitTests {

	@Test
	void newPptxAuthorSeedsItsSkillAndResolvesTheSystemReviewer() throws Exception {
		String id = Constants.AGENT_PPTX;
		assertTrue(SystemDefaultEngines.getSystemAgents().contains(id));
		try (var registry = mockStatic(SystemEngineRegistry.class);
				var workspaces = mockStatic(ModelInferenceLogsUtils.class);
				var projects = mockStatic(SecurityProjectUtils.class)) {
			registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(true);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id)).thenReturn(null);
			SystemAgentSeeder.seed(id);

			var configCaptor = ArgumentCaptor.forClass(JSONObject.class);
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(id), configCaptor.capture()));
			JSONObject config = configCaptor.getValue();
			String prompt = config.getString("system_prompt");
			workspaces.verify(() -> ModelInferenceLogsUtils.createNewWorkspaceEntry(eq(id), isNull(), eq("PPTX Agent"),
					anyString(), eq(prompt),
					argThat(resources -> resources.size() == 1
							&& Constants.SKILL_PPTX.equals(resources.getFirst().get("resource_id"))
							&& "SKILL".equals(resources.getFirst().get("resource_type")))));
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceCoreFields(eq(id), eq("PPTX Agent"),
					anyString(), eq(prompt)));
			projects.verifyNoInteractions();
			assertFalse(config.has("model_id"));

			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id))
					.thenReturn(Map.of("name", "PPTX Agent", "system_prompt", prompt));
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(id)).thenReturn(config);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(Constants.AGENT_PPTX_REVIEWER))
					.thenReturn(Map.of("name", "PPTX Reviewer", "is_active", true));
			var loaded = AgentConfigLoader.load(mock(Room.class), null, "selected-tool-model", Map.of(), Map.of(), 40,
					5, id);
			assertEquals(prompt, loaded.getAuthoredPrompt());
			assertEquals("selected-tool-model", loaded.getModelId());
			assertTrue(loaded.useDefaultAgentTools());
			assertTrue(loaded.getMcps().isEmpty());
			assertEquals(List.of(Constants.SKILL_PPTX),
					loaded.getSkills().stream().map(skill -> skill.get("skill_id")).toList());
			assertTrue(loaded.hasPptxWorkflow());
			assertEquals(1, loaded.getSubagents().size());
			var reviewer = loaded.getSubagents().getFirst();
			assertEquals(Constants.AGENT_PPTX_REVIEWER, reviewer.getWorkspaceId());
			assertEquals(loaded.getPptxWorkflow().get("reviewer_alias"), reviewer.getAlias());
			assertEquals(6, loaded.getPptxWorkflow().get("repair_turns"));
			assertEquals(600, loaded.getPptxWorkflow().get("review_timeout_seconds"));
			assertEquals(Set.of("InspectPptx", "ExecuteNodeCode"), loaded.getDisabledDefaultTools());
			assertEquals(Set.of(".claude/skills/pptx", ".semoss/pptx-workflow"), loaded.getReadOnlyPaths());
			assertNull(loaded.getResultTool());
			assertEquals(6, loaded.getFinishingTurns());
			assertTrue(loaded.getSpawnPolicy().getMaxSubagentDepth() > 0);
			assertEquals(2, loaded.getSpawnPolicy().getMaxSubagentsPerRun());
			assertEquals(1, loaded.getSpawnPolicy().getMaxSpawnsPerTurn());
		}
	}

	@Test
	void existingPptxAuthorRestoresItsSkillAndRemovesUnbundledResources() throws Exception {
		String id = Constants.AGENT_PPTX;
		try (var registry = mockStatic(SystemEngineRegistry.class);
				var workspaces = mockStatic(ModelInferenceLogsUtils.class)) {
			registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(true);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id))
					.thenReturn(Map.of("name", "Drifted author", "system_prompt", "Old prompt"));
			workspaces.when(() -> ModelInferenceLogsUtils.findWorkspaceResource(id, Constants.SKILL_PPTX, "SKILL"))
					.thenReturn(null);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceResourcesByType(id, List.of("PROJECT")))
					.thenReturn(List.of(Map.of("resource_id", "local-image-mcp")));
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceResourcesByType(id, List.of("SKILL"))).thenReturn(
					List.of(Map.of("resource_id", Constants.SKILL_PPTX), Map.of("resource_id", "app-bootstrap")));

			SystemAgentSeeder.seed(id);

			workspaces.verify(() -> ModelInferenceLogsUtils.createNewWorkspaceEntry(anyString(), nullable(String.class),
					anyString(), anyString(), anyString(), anyList()), never());
			workspaces.verify(() -> ModelInferenceLogsUtils.createNewWorkspaceResource(anyString(), eq(id),
					eq(Constants.SKILL_PPTX), eq("SKILL"), isNull()));
			workspaces.verify(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "local-image-mcp", "PROJECT"));
			workspaces.verify(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "app-bootstrap", "SKILL"));
			workspaces.verify(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, Constants.SKILL_PPTX, "SKILL"),
					never());
			var configCaptor = ArgumentCaptor.forClass(JSONObject.class);
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(id), configCaptor.capture()));
			JSONObject config = configCaptor.getValue();
			assertEquals(Constants.AGENT_PPTX_REVIEWER,
					config.getJSONArray("subagents").getJSONObject(0).getString("workspaceId"));
			assertEquals(0, config.getJSONArray("mcps").length());
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceCoreFields(eq(id), eq("PPTX Agent"),
					anyString(), eq(config.getString("system_prompt"))));
		}
	}

	@Test
	void newReviewerSeedsAnOwnerlessWorkspaceAndLoadsItsRuntimePolicy() throws Exception {
		String id = Constants.AGENT_PPTX_REVIEWER;
		assertTrue(SystemDefaultEngines.getSystemAgents().contains(id));
		try (var registry = mockStatic(SystemEngineRegistry.class);
				var workspaces = mockStatic(ModelInferenceLogsUtils.class);
				var projects = mockStatic(SecurityProjectUtils.class)) {
			registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(true);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id)).thenReturn(null);
			SystemAgentSeeder.seed(id);

			var configCaptor = ArgumentCaptor.forClass(JSONObject.class);
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(id), configCaptor.capture()));
			JSONObject config = configCaptor.getValue();
			String prompt = config.getString("system_prompt");
			workspaces.verify(() -> ModelInferenceLogsUtils.createNewWorkspaceEntry(eq(id), isNull(),
					eq("PPTX Reviewer"), anyString(), eq(prompt), eq(List.of())));
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceCoreFields(eq(id), eq("PPTX Reviewer"),
					anyString(), eq(prompt)));
			projects.verifyNoInteractions();
			assertFalse(config.has("model_id"));
			assertFalse(config.getJSONObject("tool_policy").has("parameter_defaults"));

			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id))
					.thenReturn(Map.of("name", "PPTX Reviewer", "system_prompt", prompt));
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(id)).thenReturn(config);
			var loaded = AgentConfigLoader.load(mock(Room.class), null, "selected-tool-model", Map.of(), Map.of(), 40,
					5, id);
			assertEquals(prompt, loaded.getAuthoredPrompt());
			assertEquals("selected-tool-model", loaded.getModelId());
			assertTrue(loaded.useDefaultAgentTools());
			assertEquals("InspectPptx", loaded.getResultTool());
			assertEquals(Set.of("WriteFile", "EditFile", "MultiEdit", "MoveFile", "DeleteFile", "BashCommand",
					"ExecuteNodeCode", "TodoWrite"), loaded.getDisabledDefaultTools());
			assertFalse(loaded.getDisabledDefaultTools().contains("InspectPptx"));
			assertTrue(loaded.getMcps().isEmpty());
			assertTrue(loaded.getSkills().isEmpty());
			assertTrue(loaded.getSubagents().isEmpty());
			assertTrue(loaded.getRunHooks().isEmpty());
			assertTrue(loaded.getToolHooks().isEmpty());
			assertEquals(3, loaded.getBudgets().getMaxTurns());
			assertEquals(0, loaded.getBudgets().getMaxReflections());
			assertEquals(900, loaded.getBudgets().getMaxSeconds());
			assertEquals(0, loaded.getSpawnPolicy().getMaxSubagentDepth());
			assertEquals(0, loaded.getSpawnPolicy().getMaxSubagentsPerRun());
			assertEquals(0, loaded.getSpawnPolicy().getMaxSpawnsPerTurn());
		}
	}

	@Test
	void repeatedReviewerSeedingRepairsConfigAndPrunesUnwantedResources() throws Exception {
		String id = Constants.AGENT_PPTX_REVIEWER;
		try (var registry = mockStatic(SystemEngineRegistry.class);
				var workspaces = mockStatic(ModelInferenceLogsUtils.class)) {
			registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(true);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id))
					.thenReturn(Map.of("name", "Drifted reviewer", "system_prompt", "Drifted prompt"));
			List<Map<String, Object>> staleTools = new ArrayList<>(List.of(Map.of("resource_id", "node-builder")));
			List<Map<String, Object>> staleSkills = new ArrayList<>(List.of(Map.of("resource_id", "pptx")));
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceResourcesByType(id, List.of("PROJECT")))
					.thenAnswer(call -> new ArrayList<>(staleTools));
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceResourcesByType(id, List.of("SKILL")))
					.thenAnswer(call -> new ArrayList<>(staleSkills));
			workspaces.when(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "node-builder", "PROJECT"))
					.thenAnswer(call -> {
						staleTools.clear();
						return 1;
					});
			workspaces.when(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "pptx", "SKILL"))
					.thenAnswer(call -> {
						staleSkills.clear();
						return 1;
					});

			SystemAgentSeeder.seed(id);
			SystemAgentSeeder.seed(id);

			workspaces.verify(() -> ModelInferenceLogsUtils.createNewWorkspaceEntry(anyString(), nullable(String.class),
					anyString(), anyString(), anyString(), anyList()), never());
			workspaces.verify(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "node-builder", "PROJECT"));
			workspaces.verify(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "pptx", "SKILL"));
			var configCaptor = ArgumentCaptor.forClass(JSONObject.class);
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(id), configCaptor.capture()),
					times(2));
			JSONObject first = configCaptor.getAllValues().get(0);
			assertTrue(first.similar(configCaptor.getAllValues().get(1)));
			assertEquals(0, first.getJSONArray("mcps").length());
			assertEquals(0, first.getJSONArray("skills").length());
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceCoreFields(eq(id), eq("PPTX Reviewer"),
					anyString(), eq(first.getString("system_prompt"))), times(2));
		}
	}

	@Test
	void appBuilderSeedsItsExplicitSkillsAndUnrestrictedPolicy() throws Exception {
		String id = Constants.AGENT_APP_BUILDER;
		List<String> expectedSkills = List.of("agent-run", "app-bootstrap", "app-data", "build-and-publish",
				"database", "exports", "file-uploads", "functions", "model", "pagination", "permissions", "python",
				"room", "storage", "user", "vector");
		try (var registry = mockStatic(SystemEngineRegistry.class);
				var workspaces = mockStatic(ModelInferenceLogsUtils.class);
				var projects = mockStatic(SecurityProjectUtils.class)) {
			registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(true);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id)).thenReturn(null);
			projects.when(() -> SecurityProjectUtils.getProjectTypeForId(anyString())).thenReturn("CODE");
			SystemAgentSeeder.seed(id);

			var configCaptor = ArgumentCaptor.forClass(JSONObject.class);
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(id), configCaptor.capture()));
			JSONObject config = configCaptor.getValue();
			workspaces.verify(() -> ModelInferenceLogsUtils.createNewWorkspaceEntry(eq(id), isNull(),
					eq("App Building Agent"), anyString(), eq(config.getString("system_prompt")),
					argThat(resources -> resources.size() == SystemDefaultEngines.getSystemAgentMCPs(id).size()
							+ expectedSkills.size()
							&& resources.stream().noneMatch(
									resource -> Constants.SKILL_PPTX.equals(resource.get("resource_id"))))));
			assertEquals(SystemDefaultEngines.getSystemAgentMCPs(id),
					config.getJSONArray("mcps").toList().stream().map(value -> ((Map<?, ?>) value).get("id")).toList());
			assertEquals(expectedSkills, config.getJSONArray("skills").toList().stream()
					.map(value -> ((Map<?, ?>) value).get("skill_id")).toList());
			assertFalse(config.has("tool_policy"));
			assertFalse(config.has("spawn_policy"));
			assertFalse(config.has("budgets"));
		}
	}

	@Test
	void disabledInferenceLogsDatabaseDoesNotWriteWorkspaceData() {
		try (var registry = mockStatic(SystemEngineRegistry.class);
				var workspaces = mockStatic(ModelInferenceLogsUtils.class)) {
			registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(false);
			SystemAgentSeeder.seed(Constants.AGENT_PPTX_REVIEWER);
			workspaces.verifyNoInteractions();
		}
	}
}
