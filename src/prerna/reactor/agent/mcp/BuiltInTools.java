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
package prerna.reactor.agent.mcp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;

/**
 * Registry for built-in tools that are injected into the LLM tool list at the
 * playground level. These tools are not backed by engine or MCP server
 * implementations — they signal frontend-handled interactions.
 *
 * <p>Built-in tools differ from MCP tools in that they never execute server-side
 * logic. Instead, their {@link MCPExecution} mode (e.g. {@code ASK}) tells the
 * frontend to render an interactive overlay and wait for user input.</p>
 *
 * <p>To add a new built-in tool, create a private builder method and wire it
 * into {@link #getEnabledTools(Map)} and {@link #getToolLookupEntries(Map)}.</p>
 */
public final class BuiltInTools {

	private static final Logger classLogger = LogManager.getLogger(BuiltInTools.class);

	/** Tool name for the built-in ask-user interaction tool. */
	public static final String ASK_USER_TOOL_NAME = "askUser";

	/** Room options key controlling whether the askUser tool is enabled. */
	public static final String ASK_USER_ENABLED_KEY = "askUserEnabled";

	private BuiltInTools() {
		// utility class — prevent instantiation
	}

	/**
	 * Returns the list of enabled built-in tool definitions based on the
	 * provided room options map.
	 *
	 * @param roomOptions room options map (may be {@code null})
	 * @return mutable list of tool definition maps ready for LLM consumption
	 */
	public static List<Map<String, Object>> getEnabledTools(Map<String, Object> roomOptions) {
		List<Map<String, Object>> tools = new ArrayList<>();
		if (isAskUserEnabled(roomOptions)) {
			tools.add(buildAskUserToolDefinition());
			classLogger.debug("Built-in askUser tool injected into tool list");
		}
		return tools;
	}

	/**
	 * Returns the reverse-lookup entries for all enabled built-in tools. These
	 * entries are consumed by {@link prerna.engine.impl.model.Room#updateToolResponseMeta(
	 * prerna.engine.impl.model.message.ResponseMessage)} to enrich LLM tool-call
	 * responses with platform metadata.
	 *
	 * @param roomOptions room options map (may be {@code null})
	 * @return map of tool name → lookup entry for enabled built-in tools
	 */
	public static Map<String, Map<String, Object>> getToolLookupEntries(Map<String, Object> roomOptions) {
		Map<String, Map<String, Object>> entries = new HashMap<>();
		if (isAskUserEnabled(roomOptions)) {
			entries.put(ASK_USER_TOOL_NAME, buildAskUserLookupEntry());
		}
		return entries;
	}

	/**
	 * Checks whether the given tool name is a built-in interactive tool that
	 * requires frontend user interaction and cannot be auto-executed by the
	 * agent harness.
	 *
	 * @param toolName raw tool name from the LLM response
	 * @return {@code true} if the tool requires interactive user input
	 */
	public static boolean isInteractiveBuiltIn(String toolName) {
		return ASK_USER_TOOL_NAME.equals(toolName);
	}

	// ---- Private helpers ----

	/**
	 * Reads the askUser enabled flag from room options, defaulting to
	 * {@code true} when not present.
	 *
	 * @param options room options map
	 * @return {@code true} if the askUser tool should be included
	 */
	private static boolean isAskUserEnabled(Map<String, Object> options) {
		if (options == null || !options.containsKey(ASK_USER_ENABLED_KEY)) {
			return true;
		}
		Object val = options.get(ASK_USER_ENABLED_KEY);
		if (val instanceof Boolean) {
			return (Boolean) val;
		}
		if (val != null) {
			return Boolean.parseBoolean(val.toString());
		}
		return true;
	}

	/**
	 * Builds the simplified askUser tool definition following the standard MCP
	 * tool JSON schema format.
	 *
	 * <p>The schema is intentionally minimal (~50-60 tokens) to reduce per-call
	 * token overhead. The frontend decides how to render based on whether
	 * {@code options} is present:</p>
	 * <ul>
	 *   <li>No options → free-text input</li>
	 *   <li>Options present → selection buttons or radio group</li>
	 * </ul>
	 *
	 * @return tool definition map ready to include in the aggregated tools list
	 */
	private static Map<String, Object> buildAskUserToolDefinition() {
		Map<String, Object> tool = new HashMap<>();
		tool.put("name", ASK_USER_TOOL_NAME);
		tool.put("title", "Ask User");
		tool.put("description",
				"Ask the user a question and wait for their response. "
						+ "Use when you need clarification, a choice, confirmation, "
						+ "or any user input before proceeding.");

		// inputSchema — kept minimal for token efficiency
		Map<String, Object> inputSchema = new HashMap<>();
		inputSchema.put("type", "object");

		Map<String, Object> properties = new HashMap<>();

		Map<String, Object> questionProp = new HashMap<>();
		questionProp.put("type", "string");
		questionProp.put("description", "The question to ask the user");
		properties.put("question", questionProp);

		Map<String, Object> optionsProp = new HashMap<>();
		optionsProp.put("type", "array");
		Map<String, Object> optionsItems = new HashMap<>();
		optionsItems.put("type", "string");
		optionsProp.put("items", optionsItems);
		optionsProp.put("description", "Optional list of choices to present to the user");
		properties.put("options", optionsProp);

		inputSchema.put("properties", properties);
		inputSchema.put("required", List.of("question"));
		tool.put("inputSchema", inputSchema);

		// _meta — tells the frontend this is an interactive tool
		Map<String, Object> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());

		Map<String, Object> uiMeta = new HashMap<>();
		uiMeta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.INLINE.getValue());
		meta.put(MCPUtility.SMSS_MCP_UI, uiMeta);

		tool.put("_meta", meta);
		return tool;
	}

	/**
	 * Builds the reverse-lookup entry for the askUser tool. This entry is
	 * stored in {@code toolLookupByLLMName} so that
	 * {@link prerna.engine.impl.model.Room#updateToolResponseMeta} can enrich
	 * the LLM response with platform metadata.
	 *
	 * @return lookup entry map with title, description, and _meta
	 */
	private static Map<String, Object> buildAskUserLookupEntry() {
		Map<String, Object> lookupMeta = new HashMap<>();
		lookupMeta.put(MCPUtility.SMSS_ORIGINAL_TOOL_NAME, ASK_USER_TOOL_NAME);
		lookupMeta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());

		Map<String, Object> entry = new HashMap<>();
		entry.put("title", "Ask User");
		entry.put("description", "Ask the user a question and wait for their response.");
		entry.put("_meta", lookupMeta);
		return entry;
	}
}
