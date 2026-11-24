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
package prerna.reactor.model;

import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.json.JSONException;
import org.json.JSONObject;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.AbstractWorkspaceReactor;
import prerna.om.Insight;
import prerna.reactor.agent.AgentUseAccess;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.runtime.PlatformAgentTools;
import prerna.util.Constants;
import prerna.util.ValueUtils;

/** Limits what an unattended replay can do without changing the original workspace. */
public final class RouterReplayWorkspace {
	public static final String REPLAY_RUN = "router_evaluation_replay";
	private static final Set<String> ROOM_TOOLS = Set.of("ReadFile", "WriteFile", "EditFile", "MultiEdit", "MoveFile", "DeleteFile",
			"GlobFiles", "GrepFiles", "ListDirectory", "TodoWrite", "TodoRead", "ListSkill", "LoadSkill");

	private RouterReplayWorkspace() {
	}

	record Snapshot(String fingerprint, Set<String> tools) {
	}

	static Snapshot inspect(Insight insight, String id) {
		if (id == null || Constants.AGENT_COLLABORATION.equals(id) || !AgentUseAccess.mayUseAgent(insight.getUser(), null, id) || SecurityProjectUtils.isDigitalTwin(id)) {
			return null;
		}
		var entry = ModelInferenceLogsUtils.getWorkspaceEntry(id);
		if (entry == null || Boolean.FALSE.equals(entry.get("is_active"))) {
			return null;
		}
		String raw = ValueUtils.trimToNull(entry.get("config_json"));
		JSONObject config;
		try {
			config = raw == null ? new JSONObject() : new JSONObject(raw);
		} catch (JSONException e) {
			return null;
		}
		if (hasEntries(config, "hooks") || hasEntries(config, "subagents") || hasEntries(config, "mcps")
				|| config.optJSONObject("pptx_workflow") != null || PlatformAgentTools.hasDefaultMcpOverride()) {
			return null;
		}
		var resources = ModelInferenceLogsUtils.getWorkspaceResourcesIgnoringType(id, List.of());
		if (resources == null || resources.stream().anyMatch(row -> !Set.of(AbstractWorkspaceReactor.PROMPT_RESOURCE_TYPE, AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE).contains(String.valueOf(row.get("resource_type"))))) {
			return null;
		}
		Set<String> tools = tools(config);
		return new Snapshot(ModelRouterEngine.sha256(new TreeMap<>(entry).toString() + "|" + tools.stream().sorted().toList()
				+ "|" + resources.stream().map(row -> new TreeMap<>(row).toString()).sorted().toList()), tools);
	}

	private static boolean hasEntries(JSONObject config, String key) {
		var entries = config.optJSONArray(key);
		return entries != null && !entries.isEmpty();
	}

	private static Set<String> tools(JSONObject config) {
		if (!config.optBoolean("use_default_agent_tools", true)) {
			return Set.of();
		}
		var policy = config.optJSONObject("tool_policy");
		var defaults = policy == null ? null : policy.optJSONObject("default_tools");
		var disabled = defaults == null ? null : defaults.optJSONArray("disabled");
		List<?> excluded = disabled == null ? List.of() : disabled.toList();
		return PlatformAgentTools.getDefaultToolDefinitions().stream().map(tool -> tool.get("name"))
				.filter(String.class::isInstance).map(String.class::cast).filter(ROOM_TOOLS::contains)
				.filter(name -> !excluded.contains(name)).collect(Collectors.toUnmodifiableSet());
	}

	public static boolean isReplay(AgentConfig config) {
		return config.getAgentParams().get(REPLAY_RUN) instanceof String;
	}

	public static boolean allowsTool(AgentConfig config, String name) {
		return name != null && ROOM_TOOLS.contains(name) && config.useDefaultAgentTools()
				&& !config.getDisabledDefaultTools().contains(name) && !PlatformAgentTools.hasDefaultMcpOverride();
	}

	public static void verifyRun(AgentConfig config, Insight insight) {
		if (!isReplay(config)) {
			return;
		}
		Snapshot actual = inspect(insight, config.getWorkspaceId());
		if (actual == null || !actual.fingerprint().equals(config.getAgentParams().get(REPLAY_RUN))
				|| config.isDigitalTwin() || config.hasPptxWorkflow()
				|| !config.getMcps().isEmpty() || !config.getSubagents().isEmpty()
				|| !config.getRunHooks().isEmpty() || !config.getToolHooks().isEmpty()) {
			throw new IllegalStateException("The original workspace is no longer available for replay");
		}
	}
}
