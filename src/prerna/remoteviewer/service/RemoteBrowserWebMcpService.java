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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *******************************************************************************/
package prerna.remoteviewer.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.microsoft.playwright.Page;

/**
 * Discovers and executes WebMCP tools exposed by the active remote page.
 *
 * <p>
 * WebMCP is optional and experimental. Every method therefore returns an
 * explicit support/result envelope instead of treating an unavailable browser
 * API as an error. Callers can safely fall back to normal Playwright actuation.
 */
public final class RemoteBrowserWebMcpService {

	private static final Gson GSON = new Gson();
	private static final int MAX_TOOLS = 40;
	private static final int MAX_TOOL_TEXT = 1_000;
	private static final int MAX_SCHEMA_JSON = 12_000;
	private static final int MAX_RESULT_TEXT = 4_000;

	private static final String JS_DISCOVER_TOOLS = """
			async () => {
			  const context = document.modelContext || globalThis.navigator?.modelContext;
			  if (!context || typeof context.getTools !== 'function') {
			    return {
			      supported: false,
			      tools: [],
			      message: 'This browser or page does not expose the WebMCP API.'
			    };
			  }
			  try {
			    const tools = await context.getTools();
			    return {
			      supported: true,
			      tools: tools.map(tool => ({
			        name: String(tool.name || ''),
			        title: String(tool.title || ''),
			        description: String(tool.description || ''),
			        origin: String(tool.origin || location.origin),
			        inputSchema: tool.inputSchema || { type: 'object', properties: {} },
			        annotations: tool.annotations || {}
			      })),
			      message: tools.length ? '' : 'The current page exposes no WebMCP tools.'
			    };
			  } catch (error) {
			    return {
			      supported: true,
			      tools: [],
			      message: error instanceof Error ? error.message : String(error)
			    };
			  }
			}
			""";

	private static final String JS_EXECUTE_TOOL = """
			async payload => {
			  const context = document.modelContext || globalThis.navigator?.modelContext;
			  if (!context || typeof context.getTools !== 'function'
			      || typeof context.executeTool !== 'function') {
			    return { success: false, error: 'WebMCP is unavailable on the current page.' };
			  }
			  const tools = await context.getTools();
			  const tool = tools.find(candidate =>
			    candidate.name === payload.name
			      && (!payload.origin || String(candidate.origin || location.origin) === payload.origin)
			  );
			  if (!tool) {
			    return { success: false, error: `WebMCP tool '${payload.name}' is no longer available.` };
			  }
			  try {
			    const result = await context.executeTool(tool, JSON.stringify(payload.arguments || {}));
			    let output = '';
			    if (typeof result === 'string') output = result;
			    else if (result !== undefined && result !== null) {
			      try { output = JSON.stringify(result); } catch (_) { output = String(result); }
			    }
			    return { success: true, result: output };
			  } catch (error) {
			    return {
			      success: false,
			      error: error instanceof Error ? error.message : String(error)
			    };
			  }
			}
			""";

	private RemoteBrowserWebMcpService() {
	}

	/** Returns the WebMCP capability and sanitized tool catalog for a page. */
	public static Map<String, Object> discover(Page page) {
		Map<String, Object> response = new LinkedHashMap<>();
		if (page == null || page.isClosed()) {
			response.put("supported", false);
			response.put("tools", List.of());
			response.put("message", "No active browser page is available.");
			return response;
		}

		try {
			Object raw = page.evaluate(JS_DISCOVER_TOOLS);
			if (!(raw instanceof Map<?, ?> rawMap)) {
				throw new IllegalStateException("WebMCP discovery returned an invalid response");
			}
			response.put("supported", Boolean.TRUE.equals(rawMap.get("supported")));
			response.put("tools", sanitizeTools(rawMap.get("tools")));
			response.put("message", truncate(rawMap.get("message"), MAX_TOOL_TEXT));
			return response;
		} catch (Exception e) {
			response.put("supported", false);
			response.put("tools", List.of());
			response.put("message", firstMessage(e, "WebMCP discovery failed"));
			return response;
		}
	}

	/** Executes one currently exposed tool after resolving it again by name/origin. */
	public static Map<String, Object> execute(Page page, String name, String origin,
			Map<String, Object> arguments) {
		if (page == null || page.isClosed()) {
			return failure("No active browser page is available.");
		}
		String toolName = name == null ? "" : name.trim();
		if (toolName.isBlank()) {
			return failure("A WebMCP tool name is required.");
		}

		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("name", toolName);
		payload.put("origin", origin == null ? "" : origin.trim());
		payload.put("arguments", arguments == null ? Map.of() : arguments);
		String urlBefore = page.url();
		try {
			Object raw = page.evaluate(JS_EXECUTE_TOOL, payload);
			if (!(raw instanceof Map<?, ?> rawMap)) {
				return failure("WebMCP execution returned an invalid response.");
			}
			Map<String, Object> response = new LinkedHashMap<>();
			boolean success = Boolean.TRUE.equals(rawMap.get("success"));
			response.put("success", success);
			if (success) {
				response.put("result", truncate(rawMap.get("result"), MAX_RESULT_TEXT));
			} else {
				response.put("error", truncate(rawMap.get("error"), MAX_TOOL_TEXT));
			}
			return response;
		} catch (Exception e) {
			// A WebMCP tool is allowed to navigate. Some browser builds return null as
			// specified, while others may destroy the evaluation context first. A changed
			// main-page URL is sufficient evidence that the requested tool was invoked.
			if (!page.isClosed() && !urlBefore.equals(page.url())) {
				Map<String, Object> navigated = new LinkedHashMap<>();
				navigated.put("success", true);
				navigated.put("result", "");
				navigated.put("navigated", true);
				return navigated;
			}
			return failure(firstMessage(e, "WebMCP tool execution failed"));
		}
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> sanitizeTools(Object rawTools) {
		if (!(rawTools instanceof List<?> list)) {
			return List.of();
		}
		List<Map<String, Object>> tools = new ArrayList<>();
		for (Object rawTool : list) {
			if (!(rawTool instanceof Map<?, ?> rawMap) || tools.size() >= MAX_TOOLS) {
				continue;
			}
			String name = truncate(rawMap.get("name"), MAX_TOOL_TEXT);
			if (name.isBlank()) {
				continue;
			}
			Map<String, Object> tool = new LinkedHashMap<>();
			tool.put("name", name);
			tool.put("title", truncate(rawMap.get("title"), MAX_TOOL_TEXT));
			tool.put("description", truncate(rawMap.get("description"), MAX_TOOL_TEXT));
			tool.put("origin", truncate(rawMap.get("origin"), MAX_TOOL_TEXT));
			tool.put("inputSchema", sanitizeJsonValue(rawMap.get("inputSchema"), Map.of("type", "object")));
			tool.put("annotations", sanitizeJsonValue(rawMap.get("annotations"), Map.of()));
			tools.add(tool);
		}
		return tools;
	}

	private static Object sanitizeJsonValue(Object value, Object fallback) {
		try {
			String json = GSON.toJson(value);
			if (json.length() > MAX_SCHEMA_JSON) {
				return fallback;
			}
			return GSON.fromJson(json, Object.class);
		} catch (Exception e) {
			return fallback;
		}
	}

	private static Map<String, Object> failure(String error) {
		Map<String, Object> response = new LinkedHashMap<>();
		response.put("success", false);
		response.put("error", error);
		return response;
	}

	private static String truncate(Object value, int limit) {
		String text = value == null ? "" : String.valueOf(value).trim();
		return text.length() <= limit ? text : text.substring(0, limit);
	}

	private static String firstMessage(Exception exception, String fallback) {
		String message = exception.getMessage();
		return message == null || message.isBlank() ? fallback : truncate(message, MAX_TOOL_TEXT);
	}
}
