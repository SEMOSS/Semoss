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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import prerna.reactor.agent.skill.Skill;
import prerna.reactor.agent.skill.SkillProjects;
import prerna.reactor.agent.skill.SkillProjects.SkillInfo;
import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Wraps the MCP of a skill project so it always serves two tools -
 * {@code list_<skill>_skill_files} and {@code read_<skill>_skill_file}, running
 * {@code ListSkillFiles} and {@code ReadSkillFile} - on top of whatever the
 * project defines in its own {@code mcp/} folder.
 *
 * <p>
 * This is what makes a skill usable when it is called as an MCP
 * ({@code GetMCPTools}/{@code RunMCPTool}, or a room that lists the skill as an
 * MCP): the caller lists the skill's files with their declared
 * name/description, then reads only the ones it needs. Agent runs do not come
 * through here - they stage the skill folder and load it with
 * {@code LoadSkill}, and agent and room tool aggregation skip SKILL resources.
 * A skill needs no {@code mcp/pixel_mcp.json} of its own.
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
 * The schemas and execution metadata are generated from the reactors via
 * {@link AbstractReactor#asMcpTool()}, so they cannot drift from the reactors.
 * The name, title, and description are derived from the skill's
 * {@code SKILL.md} frontmatter, so a model looking at several skills' tools in
 * one list can tell which skill holds what guidance ({@code vector} serves
 * {@code list_vector_skill_files}). The {@code project} parameter is pinned to
 * this skill as a single-value {@code enum} plus a {@code default}. The enum
 * tells the client there is exactly one legal value; the default is what makes
 * a zero-argument call work, since {@link MCPUtility#runPixelTool} substitutes
 * a parameter's {@code default} when the caller omits it - without it the
 * reactors would fall back to the insight's context project, which is not
 * necessarily this skill.
 *
 * <p>
 * A tool the project defines itself wins over the equivalent default: the
 * author's own definition is served and ours is dropped, so the published list
 * never offers the same capability twice. "Equivalent" means same name or same
 * {@code SMSS_FUNCTION_NAME} (see {@link #coversCapability}).
 */
public class SkillMCP implements IMCP {

	private static final Logger classLogger = LogManager.getLogger(SkillMCP.class);

	/**
	 * Stamped into every generated tool as {@link MCPUtility#SMSS_MCP_GENERATOR}.
	 */
	private static final String GENERATOR_ID = "SkillDefaults";

	/**
	 * Longest skill-derived segment in a generated tool name. Keeps
	 * {@code read_<key>_skill_file} within the 54 characters a 64-character
	 * provider limit leaves after the short engine-id prefix, so the name is never
	 * truncated - a truncated name would not match when the call comes back.
	 */
	private static final int MAX_NAME_KEY_LENGTH = 32;

	/** The skill project being served. */
	private final IProject project;

	/** The MCP this decorates - the project's folder-backed or remote MCP. */
	private final IMCP delegate;

	/**
	 * The generated definitions for the current {@code SKILL.md}, rebuilt when that
	 * file changes. Nothing resets a project's MCP when a skill is edited, so the
	 * file's modified time is what keeps the names and descriptions current.
	 */
	private volatile DefaultTools defaults;

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
	}

	/**
	 * A fresh, independent copy of the generated definitions. Callers mutate what
	 * {@link #getMCPTools()} hands back - {@code MCPUtility.appendEngineIdToToolsMethodName}
	 * renames every tool object in place to prefix it with the engine id - so each
	 * call has to get its own copy. Sharing one instance would prefix the cached
	 * names on the first room aggregation and double-prefix them on the next.
	 */
	private JSONArray defaultTools() {
		DefaultTools current = this.defaults;
		if (current == null || current.isStale()) {
			current = DefaultTools.build(this.project.getEngineId());
			this.defaults = current;
		}
		return new JSONArray(current.json);
	}

	/**
	 * The generated definitions, serialized, and the {@code SKILL.md} state they
	 * were built from.
	 */
	private static final class DefaultTools {

		private final Path skillFile;
		private final long lastModified;
		private final String json;

		private DefaultTools(Path skillFile, long lastModified, String json) {
			this.skillFile = skillFile;
			this.lastModified = lastModified;
			this.json = json;
		}

		/**
		 * Stats the file before reading it, so an edit that lands in between marks the
		 * result stale rather than being missed.
		 */
		private static DefaultTools build(String projectId) {
			Path skillDir = SkillProjects.resolveSkillDir(projectId);
			Path skillFile = skillDir == null ? null : skillDir.resolve(Skill.SKILL_FILE);
			long lastModified = lastModified(skillFile);
			SkillInfo info = SkillProjects.resolve(projectId);
			return new DefaultTools(skillFile, lastModified, buildDefaultTools(projectId, info).toString());
		}

		private boolean isStale() {
			return lastModified(this.skillFile) != this.lastModified;
		}
	}

	/** Modified time of {@code file}, or -1 when there is no readable file. */
	private static long lastModified(Path file) {
		if (file == null) {
			return -1L;
		}
		try {
			return Files.getLastModifiedTime(file).toMillis();
		} catch (IOException e) {
			return -1L;
		}
	}

	/**
	 * Builds the default tool definitions from the reactors that implement them,
	 * names and describes them after the skill, and pins each one's
	 * {@code project} parameter to {@code projectId}.
	 */
	private static JSONArray buildDefaultTools(String projectId, SkillInfo info) {
		String key = toolNameKey(info.slug);
		String listName = "list_" + key + "_skill_files";
		String readName = "read_" + key + "_skill_file";
		String title = titleCase(info.slug);

		StringBuilder listDescription = new StringBuilder();
		listDescription.append("List the files in the '").append(info.name).append("' skill. ");
		if (info.description != null && !info.description.isBlank()) {
			String description = info.description.trim();
			listDescription.append(description);
			if (!description.endsWith(".")) {
				listDescription.append('.');
			}
			listDescription.append(' ');
		}
		listDescription.append("Returns each file with its path, name, and description so you can read only what ")
				.append("you need; start with SKILL.md. Pass a returned filePath to ").append(readName).append('.');

		JSONObject list = new ListSkillFilesReactor().asMcpTool();
		describeTool(list, listName, "List " + title + " Skill Files", listDescription.toString());

		JSONObject read = new ReadSkillFileReactor().asMcpTool();
		describeTool(read, readName, "Read " + title + " Skill File", "Read one file from the '" + info.name
				+ "' skill in full. Pass filePath exactly as " + listName + " reports it; SKILL.md is the main guide.");
		JSONObject filePath = parameter(read, ReactorKeysEnum.FILE_PATH.getKey());
		if (filePath != null) {
			filePath.put("description", "Path of the file to read, relative to the skill folder, as reported by "
					+ listName + ". Use 'SKILL.md' for the main guide.");
			filePath.put("default", Skill.SKILL_FILE);
		}

		JSONArray tools = new JSONArray();
		for (JSONObject tool : new JSONObject[] { list, read }) {
			pinProjectParameter(tool, projectId, info.name);
			tools.put(tool);
		}
		MCPUtility.stampGenerator(tools, GENERATOR_ID);
		return tools;
	}

	/**
	 * The slug as a tool-name segment: {@code [a-z0-9_]} only, since Amazon Nova
	 * rejects hyphenated tool names, and capped at {@link #MAX_NAME_KEY_LENGTH}.
	 */
	private static String toolNameKey(String slug) {
		String key = slug.replace('-', '_');
		if (key.length() > MAX_NAME_KEY_LENGTH) {
			key = key.substring(0, MAX_NAME_KEY_LENGTH);
		}
		key = key.replaceAll("_+$", "");
		return key.isEmpty() ? "skill" : key;
	}

	/** {@code app-bootstrap} becomes {@code App Bootstrap}. */
	private static String titleCase(String slug) {
		StringBuilder title = new StringBuilder();
		for (String word : slug.split("-")) {
			if (word.isEmpty()) {
				continue;
			}
			if (title.length() > 0) {
				title.append(' ');
			}
			title.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
		}
		return title.toString();
	}

	private static void describeTool(JSONObject tool, String name, String title, String description) {
		tool.put("name", name);
		tool.put("title", title);
		tool.put("description", description);
		JSONObject inputSchema = tool.optJSONObject("inputSchema");
		if (inputSchema != null) {
			inputSchema.put("title", name + "_Arguments");
		}
	}

	/** The schema of the tool's {@code key} parameter, or null when it has none. */
	private static JSONObject parameter(JSONObject tool, String key) {
		JSONObject inputSchema = tool.optJSONObject("inputSchema");
		JSONObject properties = inputSchema == null ? null : inputSchema.optJSONObject("properties");
		return properties == null ? null : properties.optJSONObject(key);
	}

	/**
	 * Rewrites the tool's {@code project} parameter so this skill is the only value
	 * it can take, and the value it takes when the caller says nothing.
	 */
	private static void pinProjectParameter(JSONObject tool, String projectId, String skillName) {
		JSONObject projectProperty = parameter(tool, ReactorKeysEnum.PROJECT.getKey());
		if (projectProperty == null) {
			classLogger.warn("SkillMCP: tool '{}' has no '{}' parameter to pin to skill '{}'", tool.optString("name"),
					ReactorKeysEnum.PROJECT.getKey(), projectId);
			return;
		}
		projectProperty.put("enum", new JSONArray().put(projectId));
		projectProperty.put("default", projectId);
		projectProperty.put("description", "The " + skillName + " skill (" + projectId + "). Always this value.");
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
			if (coversCapability(tools, defaultTool)) {
				// the project's own definition of this capability wins
				classLogger.info("SkillMCP: skill '{}' defines its own '{}', not adding the default",
						this.project.getEngineId(), defaultTool.optString("name"));
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
		if (defaultTool != null && !projectDefinesTool(defaultTool)) {
			JSONObject inputSchema = defaultTool.optJSONObject("inputSchema");
			JSONObject properties = inputSchema == null ? null : inputSchema.optJSONObject("properties");
			return MCPUtility.runPixelTool(this.project, insight, functionName(defaultTool),
					properties == null ? new JSONObject() : properties, params);
		}
		return this.delegate.callTool(toolName, params, insight);
	}

	/**
	 * The generated default called {@code name}, or null when there is none. The
	 * reactor names ({@code ListSkillFiles}, {@code ReadSkillFile}) also match -
	 * they were the default tool names before the names were derived from the
	 * skill, so a conversation that learned them keeps working.
	 */
	private JSONObject findDefaultTool(String name) {
		JSONArray defaults = defaultTools();
		for (int i = 0; i < defaults.length(); i++) {
			JSONObject tool = defaults.getJSONObject(i);
			if (name.equals(tool.optString("name")) || name.equals(functionName(tool))) {
				return tool;
			}
		}
		return null;
	}

	/**
	 * True when the project already covers {@code defaultTool} in its own
	 * {@code mcp/} folder, in which case that definition is the one being served
	 * and the execution has to go to the delegate.
	 */
	private boolean projectDefinesTool(JSONObject defaultTool) {
		JSONObject delegateTools = this.delegate.getMCPTools();
		JSONArray tools = delegateTools == null ? null : delegateTools.optJSONArray("tools");
		return coversCapability(tools, defaultTool);
	}

	/** The reactor a tool runs: its {@code SMSS_FUNCTION_NAME}, else its name. */
	private static String functionName(JSONObject tool) {
		JSONObject meta = tool.optJSONObject("_meta");
		String name = tool.optString("name");
		return meta == null ? name : meta.optString(MCPUtility.SMSS_FUNCTION_NAME, name);
	}

	/**
	 * True when {@code tools} already covers the capability {@code defaultTool}
	 * provides - either as a tool of the same name, or as a tool under a different
	 * name that runs the same reactor via {@code SMSS_FUNCTION_NAME}. Matching on
	 * name alone would treat a project that renames these tools as unrelated and
	 * publish the default beside them, offering the same capability twice.
	 */
	private static boolean coversCapability(JSONArray tools, JSONObject defaultTool) {
		if (tools == null) {
			return false;
		}
		String name = defaultTool.optString("name");
		String function = functionName(defaultTool);
		for (int i = 0; i < tools.length(); i++) {
			JSONObject tool = tools.optJSONObject(i);
			if (tool == null) {
				continue;
			}
			if (name.equals(tool.optString("name", null))) {
				return true;
			}
			JSONObject meta = tool.optJSONObject("_meta");
			if (meta != null && function.equals(meta.optString(MCPUtility.SMSS_FUNCTION_NAME, null))) {
				return true;
			}
		}
		return false;
	}
}
