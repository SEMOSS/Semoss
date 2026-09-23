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
package prerna.engine.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IMCP;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.AssetUtility;

class SkillMCPUnitTests {

	private static final String PROJECT_ID = "app-bootstrap";

	@TempDir
	Path assetsDir;

	private MockedStatic<AssetUtility> assets;
	private MockedStatic<SecurityProjectUtils> security;
	private IProject project;
	private IMCP delegate;
	private JSONArray delegateTools;

	@BeforeEach
	void setUp() throws Exception {
		writeSkill(PROJECT_ID, "Use when scaffolding a platform app");
		assets = mockStatic(AssetUtility.class);
		assets.when(() -> AssetUtility.getProjectAssetsFolder(PROJECT_ID)).thenReturn(assetsDir.toString());
		security = mockStatic(SecurityProjectUtils.class);
		security.when(() -> SecurityProjectUtils.getProjectDisplayNameForId(PROJECT_ID)).thenReturn(PROJECT_ID);

		project = mock(IProject.class);
		when(project.getEngineId()).thenReturn(PROJECT_ID);
		delegateTools = new JSONArray();
		delegate = mock(IMCP.class);
		// a fresh map per call, like InternalMCP - SkillMCP appends to what it gets
		when(delegate.getMCPTools())
				.thenAnswer(inv -> new JSONObject().put("tools", new JSONArray(delegateTools.toString())));
	}

	@AfterEach
	void tearDown() {
		security.close();
		assets.close();
	}

	@Test
	void namesAndDescribesTheDefaultsAfterTheSkill() {
		JSONArray tools = new SkillMCP(project, delegate).getMCPTools().getJSONArray("tools");

		assertEquals(List.of("list_app_bootstrap_skill_files", "read_app_bootstrap_skill_file"), names(tools));
		JSONObject list = tools.getJSONObject(0);
		JSONObject read = tools.getJSONObject(1);
		assertEquals("List App Bootstrap Skill Files", list.getString("title"));
		assertTrue(list.getString("description").contains("Use when scaffolding a platform app."));
		assertTrue(read.getString("description").contains("list_app_bootstrap_skill_files"));
		assertEquals("ListSkillFiles", list.getJSONObject("_meta").getString(MCPUtility.SMSS_FUNCTION_NAME));
		assertEquals("ReadSkillFile", read.getJSONObject("_meta").getString(MCPUtility.SMSS_FUNCTION_NAME));

		for (JSONObject tool : List.of(list, read)) {
			JSONObject projectParam = property(tool, "project");
			assertEquals(List.of(PROJECT_ID), projectParam.getJSONArray("enum").toList());
			assertEquals(PROJECT_ID, projectParam.getString("default"));
		}
		assertEquals("SKILL.md", property(read, "filePath").getString("default"));
	}

	@Test
	void aToolTheProjectDefinesReplacesTheEquivalentDefault() {
		delegateTools.put(new JSONObject().put("name", "browse_bootstrap").put("_meta",
				new JSONObject().put(MCPUtility.SMSS_FUNCTION_NAME, "ListSkillFiles")));
		SkillMCP mcp = new SkillMCP(project, delegate);

		assertEquals(List.of("browse_bootstrap", "read_app_bootstrap_skill_file"),
				names(mcp.getMCPTools().getJSONArray("tools")));

		mcp.callTool("browse_bootstrap", Map.of(), null);
		verify(delegate).callTool(eq("browse_bootstrap"), anyMap(), isNull());
	}

	@Test
	void generatedAndLegacyNamesRunTheReactor() {
		try (MockedStatic<MCPUtility> utility = mockStatic(MCPUtility.class, CALLS_REAL_METHODS)) {
			utility.when(() -> MCPUtility.runPixelTool(eq(project), isNull(), anyString(), any(JSONObject.class),
					anyMap())).thenReturn("ok");
			SkillMCP mcp = new SkillMCP(project, delegate);

			// prefixed the way a length-limited provider sees it
			assertEquals("ok", mcp.callTool("aappboots_list_app_bootstrap_skill_files", Map.of(), null));
			// the name served before tool names were derived from the skill
			assertEquals("ok", mcp.callTool("ReadSkillFile", Map.of("filePath", "SKILL.md"), null));

			utility.verify(() -> MCPUtility.runPixelTool(eq(project), isNull(), eq("ListSkillFiles"),
					any(JSONObject.class), anyMap()));
			utility.verify(() -> MCPUtility.runPixelTool(eq(project), isNull(), eq("ReadSkillFile"),
					any(JSONObject.class), anyMap()));
		}
		verify(delegate, never()).callTool(anyString(), anyMap(), any());
	}

	@Test
	void rebuildsTheDefaultsWhenSkillMdChanges() throws Exception {
		SkillMCP mcp = new SkillMCP(project, delegate);
		assertTrue(listDescription(mcp).contains("scaffolding"));

		Path skillFile = writeSkill(PROJECT_ID, "Use when wiring the Insight lifecycle");
		Files.setLastModifiedTime(skillFile,
				FileTime.fromMillis(Files.getLastModifiedTime(skillFile).toMillis() + 5_000));

		assertTrue(listDescription(mcp).contains("wiring the Insight lifecycle"));
	}

	@Test
	void capsTheSkillSegmentOfLongNames() throws Exception {
		writeSkill("design-system-review-for-mobile-apps-and-sites", "Long.");

		List<String> names = names(new SkillMCP(project, delegate).getMCPTools().getJSONArray("tools"));

		// cut at 32 characters lands just after an underscore, which is dropped
		assertEquals(List.of("list_design_system_review_for_mobile_skill_files",
				"read_design_system_review_for_mobile_skill_file"), names);
	}

	private Path writeSkill(String name, String description) throws Exception {
		Path skillDir = Files.createDirectories(assetsDir.resolve("public"));
		return Files.writeString(skillDir.resolve("SKILL.md"),
				"---\nname: " + name + "\ndescription: " + description + "\n---\n\n# Skill\n");
	}

	private static String listDescription(SkillMCP mcp) {
		return mcp.getMCPTools().getJSONArray("tools").getJSONObject(0).getString("description");
	}

	private static JSONObject property(JSONObject tool, String key) {
		return tool.getJSONObject("inputSchema").getJSONObject("properties").getJSONObject(key);
	}

	private static List<String> names(JSONArray tools) {
		return tools.toList().stream().map(tool -> (String) ((Map<?, ?>) tool).get("name")).toList();
	}
}
