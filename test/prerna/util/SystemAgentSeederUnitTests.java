package prerna.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
			var loaded = AgentConfigLoader.load(mock(Room.class), null, "selected-tool-model", Map.of(), Map.of(),
					40, 5, id);
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
					.thenAnswer(call -> { staleTools.clear(); return 1; });
			workspaces.when(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "pptx", "SKILL"))
					.thenAnswer(call -> { staleSkills.clear(); return 1; });

			SystemAgentSeeder.seed(id);
			SystemAgentSeeder.seed(id);

			workspaces.verify(() -> ModelInferenceLogsUtils.createNewWorkspaceEntry(anyString(), nullable(String.class),
					anyString(), anyString(), anyString(), anyList()), never());
			workspaces.verify(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "node-builder", "PROJECT"));
			workspaces.verify(() -> ModelInferenceLogsUtils.deleteWorkspaceResource(id, "pptx", "SKILL"));
			var configCaptor = ArgumentCaptor.forClass(JSONObject.class);
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(id), configCaptor.capture()), times(2));
			JSONObject first = configCaptor.getAllValues().get(0);
			assertTrue(first.similar(configCaptor.getAllValues().get(1)));
			assertEquals(0, first.getJSONArray("mcps").length());
			assertEquals(0, first.getJSONArray("skills").length());
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceCoreFields(eq(id), eq("PPTX Reviewer"),
					anyString(), eq(first.getString("system_prompt"))), times(2));
		}
	}

	@Test
	void appBuilderRetainsItsPlatformResourcesAndUnrestrictedPolicy() throws Exception {
		String id = Constants.AGENT_APP_BUILDER;
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
					argThat(resources -> resources.size() == SystemDefaultEngines.getSystemAgentMCPs().size()
							+ SystemDefaultEngines.getSystemSkills().size())));
			assertEquals(SystemDefaultEngines.getSystemAgentMCPs(), config.getJSONArray("mcps").toList().stream()
					.map(value -> ((Map<?, ?>) value).get("id")).toList());
			assertEquals(SystemDefaultEngines.getSystemSkills(), config.getJSONArray("skills").toList().stream()
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
