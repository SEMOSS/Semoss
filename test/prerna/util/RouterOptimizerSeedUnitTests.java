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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;

/** The router-optimizer system agent: its seeded config parses, and its tools file and playbook skill exist. */
class RouterOptimizerSeedUnitTests {

	private static final String PROJECT = "project/platform__";

	@Test
	void seedsTheAgentWithItsToolsSkillAndBudgets() throws Exception {
		String id = Constants.AGENT_ROUTER_OPTIMIZER;
		assertTrue(SystemDefaultEngines.getSystemAgents().contains(id));
		try (var registry = mockStatic(SystemEngineRegistry.class);
				var workspaces = mockStatic(ModelInferenceLogsUtils.class);
				var projects = mockStatic(SecurityProjectUtils.class)) {
			registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(true);
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id)).thenReturn(null);
			projects.when(() -> SecurityProjectUtils.getProjectTypeForId(anyString())).thenReturn("CODE");
			SystemAgentSeeder.seed(id);

			var captor = ArgumentCaptor.forClass(JSONObject.class);
			workspaces.verify(() -> ModelInferenceLogsUtils.updateWorkspaceConfigJson(eq(id), captor.capture()));
			JSONObject config = captor.getValue();
			workspaces.verify(() -> ModelInferenceLogsUtils.createNewWorkspaceEntry(eq(id), isNull(), eq("Router Optimizer"),
					anyString(), eq(config.getString("system_prompt")), anyList()));
			assertTrue(config.getString("system_prompt").contains("ApplyRouterCandidate"));
			assertEquals("router-tools", config.getJSONArray("mcps").getJSONObject(0).getString("id"));
			assertEquals("router-playbook", config.getJSONArray("skills").getJSONObject(0).getString("skill_id"));
			// the live router is reachable only through ApplyRouterCandidate: no shell, no code, no UpdateModelRouterConfig
			assertTrue(config.getJSONObject("tool_policy").getJSONObject("default_tools").getJSONArray("disabled").toList()
					.containsAll(List.of("BashCommand", "ExecuteNodeCode", "ExecutePythonCode")));
		}
	}

	@Test
	void theToolsFileAndPlaybookAreThere() throws Exception {
		JSONArray tools = new JSONObject(
				Files.readString(Path.of(PROJECT + "router-tools/app_root/version/assets/mcp/pixel_mcp.json"))).getJSONArray("tools");
		assertEquals(List.of("GetRouterEvidence", "GetModelRouterConfig", "CreateSkill",
				"ReadSkillFile", "WriteRouterCandidate", "EvaluateRouterCandidate", "ApplyRouterCandidate"),
				tools.toList().stream().map(t -> ((java.util.Map<?, ?>) t).get("name")).toList());
		assertFalse(tools.toString().contains("UpdateModelRouterConfig"));
		String skill = Files.readString(Path.of(PROJECT + "router-playbook/app_root/version/assets/public/SKILL.md"));
		assertTrue(skill.startsWith("---\nname: router-playbook\n"));
		for (String needed : List.of("ApplyRouterCandidate", "EvaluateRouterCandidate", "CreateSkill",
				"ReadSkillFile", "taskSet=VAL")) {
			assertTrue(skill.contains(needed), needed);
		}
	}
}
