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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

class SystemDefaultEnginesUnitTests {

	@Test
	void dataWorkbenchAgentsReceiveAnalysisSkillsAndReactorHelp() {
		assertTrue(SystemDefaultEngines.getSystemAgents().contains(Constants.AGENT_DATABASE_EXPLORER));
		assertTrue(SystemDefaultEngines.getSystemAgents().contains(Constants.AGENT_NOTEBOOK_ANALYST));
		assertEquals(List.of("database", "python", "pagination", "exports"),
				SystemDefaultEngines.getSystemAgentSkills(Constants.AGENT_DATABASE_EXPLORER));
		assertEquals(List.of("python", "database", "file-uploads", "storage", "exports"),
				SystemDefaultEngines.getSystemAgentSkills(Constants.AGENT_NOTEBOOK_ANALYST));
		assertEquals(List.of("reactor-help"),
				SystemDefaultEngines.getSystemAgentMCPs(Constants.AGENT_DATABASE_EXPLORER));
		assertEquals(List.of("reactor-help"),
				SystemDefaultEngines.getSystemAgentMCPs(Constants.AGENT_NOTEBOOK_ANALYST));
	}

	@Test
	void workflowAutomationBuilderUsesOnlyItsOwnSkillAndNoMcps() {
		assertTrue(SystemDefaultEngines.getSystemSkills().contains(Constants.SKILL_WORKFLOW_AUTOMATION));
		assertTrue(SystemDefaultEngines.getSystemAgents().contains(Constants.AGENT_WORKFLOW_AUTOMATION_BUILDER));
		assertEquals(List.of(Constants.SKILL_WORKFLOW_AUTOMATION),
				SystemDefaultEngines.getSystemAgentSkills(Constants.AGENT_WORKFLOW_AUTOMATION_BUILDER));
		assertTrue(SystemDefaultEngines.getSystemAgentMCPs(Constants.AGENT_WORKFLOW_AUTOMATION_BUILDER).isEmpty());
	}

	/**
	 * Spelled out rather than compared against the platform lists. Each system
	 * agent is meant to receive a deliberate subset, so an assertion derived from
	 * the same source it is checking would accept any future widening.
	 */
	@Test
	void appBuilderReceivesExactlyItsOwnSkills() {
		assertEquals(List.of(Constants.SKILL_AGENT_RUN, Constants.SKILL_APP_BOOTSTRAP, Constants.SKILL_APP_DATA,
				Constants.SKILL_BUILD_AND_PUBLISH, Constants.SKILL_DATABASE, Constants.SKILL_EXPORTS,
				Constants.SKILL_FILE_UPLOADS, Constants.SKILL_FRONTEND_DESIGN, Constants.SKILL_FUNCTIONS,
				Constants.SKILL_MCP, Constants.SKILL_MODEL, Constants.SKILL_PAGINATION, Constants.SKILL_PERMISSIONS,
				Constants.SKILL_PYTHON, Constants.SKILL_ROOM, Constants.SKILL_STORAGE, Constants.SKILL_USER,
				Constants.SKILL_VECTOR),
				SystemDefaultEngines.getSystemAgentSkills(Constants.AGENT_APP_BUILDER));
	}

	/**
	 * UI-driven MCPs open a sidebar app and wait on a person, so they are cataloged
	 * and invokable without being seeded onto a headless agent.
	 */
	@Test
	void appBuilderReceivesOnlyHeadlessMcps() {
		assertEquals(List.of(Constants.MCP_DATABASE_MAKER, Constants.MCP_NODE_BUILDER, Constants.MCP_REACTOR_HELP),
				SystemDefaultEngines.getSystemAgentMCPs(Constants.AGENT_APP_BUILDER));
	}

	@Test
	void everySystemMcpHasBootstrapAssets() {
		Path projectRoot = Path.of("project");
		for (String mcpId : SystemDefaultEngines.getSystemMCPs()) {
			String projectName = "platform__" + mcpId;
			assertTrue(Files.isRegularFile(projectRoot.resolve(projectName + ".smss")),
					mcpId + " must package its project SMSS");

			Path mcpRoot = projectRoot.resolve(projectName).resolve("app_root/version/assets/mcp");
			assertTrue(Files.isRegularFile(mcpRoot.resolve("pixel_mcp.json"))
					|| Files.isRegularFile(mcpRoot.resolve("py_mcp.json")),
					mcpId + " must package a Pixel or Python MCP definition");
		}
	}

	@Test
	void memoryMcpPackagesEveryMemoryTool() throws IOException {
		Path mcpFile = Path.of("project", "platform__" + Constants.MCP_MEMORY, "app_root", "version", "assets", "mcp",
				"pixel_mcp.json");
		JSONArray tools = new JSONObject(Files.readString(mcpFile)).getJSONArray("tools");
		Set<String> toolNames = new HashSet<>();
		JSONObject searchMemories = null;
		for (int i = 0; i < tools.length(); i++) {
			JSONObject tool = tools.getJSONObject(i);
			String toolName = tool.getString("name");
			toolNames.add(toolName);
			if ("SearchMemories".equals(toolName)) {
				searchMemories = tool;
			}
		}

		assertEquals(Set.of("AddMemory", "SearchMemories", "ListMemories", "EditMemory", "DeleteMemory",
				"ListMemoryAudit", "CreateActionItem", "UpdateActionItemStatus", "ListActionItems",
				"CompactMemories", "PromoteMemoryToWorkspace", "GetMemoryMeta", "GetMemoryMetaValues",
				"GetMyMemorySettings", "SetMyMemorySettings"), toolNames);
		assertEquals(toolNames.size(), tools.length(), "Memory MCP tool names must be unique");
		assertTrue(searchMemories != null
				&& searchMemories.getString("description").contains("Before answering any question"),
				"SearchMemories must package its recall guidance");
	}

	/**
	 * The narrowing that matters: an agent is seeded from its own list, never from
	 * the full platform catalog. Cataloging every skill stays correct.
	 */
	@Test
	void noAgentReceivesTheWholePlatformCatalog() {
		assertTrue(SystemDefaultEngines.getSystemSkills().contains(Constants.SKILL_WORKFLOW_AUTOMATION));
		for (String agentId : SystemDefaultEngines.getSystemAgents()) {
			assertNotEquals(SystemDefaultEngines.getSystemSkills(), SystemDefaultEngines.getSystemAgentSkills(agentId),
					agentId + " must be seeded from its own skill list");
			assertNotEquals(SystemDefaultEngines.getSystemMCPs(), SystemDefaultEngines.getSystemAgentMCPs(agentId),
					agentId + " must be seeded from its own MCP list");
			assertTrue(
					SystemDefaultEngines.getSystemSkills()
							.containsAll(SystemDefaultEngines.getSystemAgentSkills(agentId)),
					agentId + " cannot be seeded a skill the platform does not catalog");
			assertTrue(
					SystemDefaultEngines.getSystemMCPs().containsAll(SystemDefaultEngines.getSystemAgentMCPs(agentId)),
					agentId + " cannot be seeded an MCP the platform does not catalog");
		}
	}

	/** An agent with no entry gets nothing rather than a default set. */
	@Test
	void unknownAgentsReceiveNothing() {
		assertTrue(SystemDefaultEngines.getSystemAgentSkills("not-an-agent").isEmpty());
		assertTrue(SystemDefaultEngines.getSystemAgentMCPs("not-an-agent").isEmpty());
	}
}
