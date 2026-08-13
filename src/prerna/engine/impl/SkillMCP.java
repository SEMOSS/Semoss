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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.IMCP;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.skill.ListSkillFilesReactor;
import prerna.reactor.agent.skill.ReadSkillFileReactor;
import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Wraps the MCP of a skill project so it always serves two tools -
 * {@code ListSkillFiles} and {@code ReadSkillFile} - on top of whatever the
 * project defines in its own {@code mcp/} folder.
 *
 * <p>
 * This is what makes a skill usable by an external MCP client: the client lists
 * the skill's files with their declared name/description, then reads only the
 * ones it needs, instead of the whole folder having to be staged somewhere
 * first. Skill projects ship no {@code mcp/pixel_mcp.json}, so without this a
 * skill served as an MCP would publish nothing.
 *
 * <p>
 * A decorator rather than a flavor of {@link InternalMCP}: {@code InternalMCP}
 * also backs rooms and insight folders and should not learn what a skill is,
 * and wrapping instead composes over {@link RemoteMCP} too, for a skill project
 * that points at a remote endpoint. {@code Project.getProjectMCP()} applies it
 * for {@code SKILL}-type projects, and every MCP entry point routes through
 * there - {@code MCPFactory.build} hands back the project itself for an
 * {@link IProject}, whose {@code getMCPTools}/{@code callTool} both delegate to
 * that method.
 *
 * <p>
 * The two tool definitions are generated from the reactors themselves via
 * {@link AbstractReactor#asMcpTool()}, so their schemas and descriptions cannot
 * drift from the reactors, with one edit applied on top: the {@code project}
 * parameter is pinned to this skill as a single-value {@code enum} plus a
 * {@code default}. The enum tells the client there is exactly one legal value;
 * the default is what makes a zero-argument call work, since
 * {@link MCPUtility#runPixelTool} substitutes a parameter's {@code default}
 * when the caller omits it - without it the reactors would fall back to the
 * insight's context project, which is not necessarily this skill.
 *
 * <p>
 * A tool the project defines itself wins over the equivalent default: the
 * author's own definition is served and ours is dropped, so the published list
 * never offers the same capability twice. "Equivalent" means same name or same
 * {@code SMSS_FUNCTION_NAME}, which is how the shipped platform skills replace
 * these with subject-specific names (see {@link #coversFunction}).
 */
public class SkillMCP implements IMCP {

	private static final Logger classLogger = LogManager.getLogger(SkillMCP.class);

	/**
	 * Stamped into every generated tool as {@link MCPUtility#SMSS_MCP_GENERATOR}.
	 */
	private static final String GENERATOR_ID = "SkillDefaults";

	/** The skill project being served. */
	private final IProject project;

	/** The MCP this decorates - the project's folder-backed or remote MCP. */
	private final IMCP delegate;

	/**
	 * The generated definitions, serialized. Held as text rather than as a
	 * {@link JSONArray} because callers mutate what {@link #getMCPTools()} hands
	 * back - {@code MCPUtility.appendEngineIdToToolsMethodName} renames every tool
	 * object in place to prefix it with the engine id - so each call has to get its
	 * own copy. Sharing one instance would prefix the cached names on the first
	 * room aggregation and double-prefix them on the next.
	 */
	private final String defaultToolsJson;

	/**
	 * @param project  the skill project whose id the generated tools are pinned to
	 * @param delegate the MCP to decorate, never null
	 */
	public SkillMCP(IProject project, IMCP delegate) {
		if (project == null) {
			throw new IllegalArgumentException("A skill project is required to build a skill MCP");
		}
		if (delegate == null) {
			throw new IllegalArgumentException("A delegate MCP is required to build a skill MCP");
		}
		this.project = project;
		this.delegate = delegate;
		this.defaultToolsJson = buildDefaultTools(project.getEngineId()).toString();
	}

	/** A fresh, independent copy of the generated definitions. */
	private JSONArray defaultTools() {
		return new JSONArray(this.defaultToolsJson);
	}

	/**
	 * Builds the default tool definitions from the reactors that implement them and
	 * pins each one's {@code project} parameter to {@code projectId}.
	 */
	private static JSONArray buildDefaultTools(String projectId) {
		JSONArray tools = new JSONArray();
		List<AbstractReactor> reactors = Arrays.asList(new ListSkillFilesReactor(), new ReadSkillFileReactor());
		for (AbstractReactor reactor : reactors) {
			JSONObject tool = reactor.asMcpTool();
			pinProjectParameter(tool, projectId);
			tools.put(tool);
		}
		MCPUtility.stampGenerator(tools, GENERATOR_ID);
		return tools;
	}

	/**
	 * Rewrites the tool's {@code project} parameter so this skill is the only value
	 * it can take, and the value it takes when the caller says nothing.
	 */
	private static void pinProjectParameter(JSONObject tool, String projectId) {
		JSONObject inputSchema = tool.optJSONObject("inputSchema");
		JSONObject properties = inputSchema == null ? null : inputSchema.optJSONObject("properties");
		JSONObject projectProperty = properties == null ? null
				: properties.optJSONObject(ReactorKeysEnum.PROJECT.getKey());
		if (projectProperty == null) {
			classLogger.warn("SkillMCP: tool '{}' has no '{}' parameter to pin to skill '{}'", tool.optString("name"),
					ReactorKeysEnum.PROJECT.getKey(), projectId);
			return;
		}
		projectProperty.put("enum", new JSONArray().put(projectId));
		projectProperty.put("default", projectId);
		projectProperty.put("description", "This skill (" + projectId + "). Always this value.");
	}

	@Override
	public JSONObject initMCP(String protocolVersion) {
		return this.delegate.initMCP(protocolVersion);
	}

	@Override
	public JSONObject getMCPResources() {
		return this.delegate.getMCPResources();
	}

	@Override
	public JSONObject getMCPResourcesTemplates() {
		return this.delegate.getMCPResourcesTemplates();
	}

	@Override
	public JSONObject getMCPPrompts() {
		return this.delegate.getMCPPrompts();
	}

	/**
	 * The delegate's tools plus every default the project has not defined itself.
	 * The delegate's {@code _meta} (engine id/name/type) is left untouched.
	 */
	@Override
	public JSONObject getMCPTools() {
		JSONObject toolMap = this.delegate.getMCPTools();
		if (toolMap == null) {
			toolMap = new JSONObject();
		}
		JSONArray tools = toolMap.optJSONArray("tools");
		if (tools == null) {
			tools = new JSONArray();
			toolMap.put("tools", tools);
		}
		JSONArray defaults = defaultTools();
		for (int i = 0; i < defaults.length(); i++) {
			JSONObject defaultTool = defaults.getJSONObject(i);
			String name = defaultTool.optString("name");
			if (coversFunction(tools, name)) {
				// the project's own definition of this capability wins
				classLogger.info("SkillMCP: skill '{}' defines its own '{}', not adding the default",
						this.project.getEngineId(), name);
				continue;
			}
			tools.put(defaultTool);
		}
		return toolMap;
	}

	@Override
	public Object callTool(String toolName, Map<String, Object> params, Insight insight) {
		if (toolName == null || toolName.trim().isEmpty()) {
			throw new IllegalArgumentException("Tool name must be passed in to execute the mcp tool");
		}
		// the caller may hold the engine-id-prefixed name; the delegate strips it
		// itself, so only match on the stripped form and hand the delegate the original
		String strippedName = MCPUtility.removeEngineIdFromToolsMethodName(this.project.getEngineId(), toolName.trim());
		JSONObject defaultTool = findDefaultTool(strippedName);
		if (defaultTool != null && !projectDefinesTool(strippedName)) {
			JSONObject inputSchema = defaultTool.optJSONObject("inputSchema");
			JSONObject properties = inputSchema == null ? null : inputSchema.optJSONObject("properties");
			JSONObject meta = defaultTool.optJSONObject("_meta");
			String functionName = meta == null ? strippedName
					: meta.optString(MCPUtility.SMSS_FUNCTION_NAME, strippedName);
			return MCPUtility.runPixelTool(this.project, insight, functionName,
					properties == null ? new JSONObject() : properties, params);
		}
		return this.delegate.callTool(toolName, params, insight);
	}

	/** The generated default carrying {@code name}, or null when there is none. */
	private JSONObject findDefaultTool(String name) {
		JSONArray defaults = defaultTools();
		for (int i = 0; i < defaults.length(); i++) {
			JSONObject tool = defaults.getJSONObject(i);
			if (name.equals(tool.optString("name"))) {
				return tool;
			}
		}
		return null;
	}

	/**
	 * True when the project already covers the default named {@code name} in its
	 * own {@code mcp/} folder, in which case that definition is the one being
	 * served and the execution has to go to the delegate.
	 */
	private boolean projectDefinesTool(String name) {
		JSONObject delegateTools = this.delegate.getMCPTools();
		JSONArray tools = delegateTools == null ? null : delegateTools.optJSONArray("tools");
		return coversFunction(tools, name);
	}

	/**
	 * True when {@code tools} already covers the capability the default named
	 * {@code name} provides - either as a tool of that name, or as a tool under a
	 * different name that runs the same reactor via {@code SMSS_FUNCTION_NAME}.
	 *
	 * <p>
	 * The function check is what lets a skill rename these tools for discovery. The
	 * shipped platform skills publish subject-specific names - {@code vector}
	 * serves {@code list_vector_skill_files} rather than a bare
	 * {@code ListSkillFiles}, so a client scanning tool names can tell which skill
	 * holds the vector guidance. Matching on name alone would treat those as
	 * unrelated and publish the generic default beside them, offering the same
	 * capability twice.
	 */
	private static boolean coversFunction(JSONArray tools, String name) {
		if (tools == null || name == null || name.isEmpty()) {
			return false;
		}
		for (int i = 0; i < tools.length(); i++) {
			JSONObject tool = tools.optJSONObject(i);
			if (tool == null) {
				continue;
			}
			if (name.equals(tool.optString("name", null))) {
				return true;
			}
			JSONObject meta = tool.optJSONObject("_meta");
			if (meta != null && name.equals(meta.optString(MCPUtility.SMSS_FUNCTION_NAME, null))) {
				return true;
			}
		}
		return false;
	}
}
