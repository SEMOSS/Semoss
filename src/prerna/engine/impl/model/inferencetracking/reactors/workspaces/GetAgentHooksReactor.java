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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.agent.hooks.AgentHookRegistry;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reactor that returns the current agent hook list for a workspace plus the set
 * of hook kinds the server recognizes. Pixel: {@code GetAgentHooks}.
 *
 * <p>
 * Input keys:
 * <ul>
 * <li>{@code workspaceId} - required</li>
 * </ul>
 *
 * <p>
 * Returns a {@link PixelDataType#MAP} shaped:
 * 
 * <pre>
 * {
 *   "hooks":      [ { "kind": "...", ... }, ... ],
 *   "knownKinds": [ "pixel", "git_commit", "log_tools" ]
 * }
 * </pre>
 *
 * <p>
 * {@code hooks} is the current {@code CONFIG_JSON.hooks[]} array as stored
 * (empty list if missing). {@code knownKinds} is the snapshot of
 * {@link AgentHookRegistry#knownKinds()} at request time - gives the FE the
 * dropdown options for "add new hook" without a second round-trip.
 *
 * <p>
 * Read-only; requires view (not edit) access to the workspace.
 */
public class GetAgentHooksReactor extends AbstractWorkspaceReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAgentHooksReactor.class);

	public GetAgentHooksReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		if (workspaceId == null || workspaceId.trim().isEmpty()) {
			throw new IllegalArgumentException("workspaceId is required");
		}

		Map<String, Object> currentRow = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (currentRow == null) {
			throw new IllegalArgumentException("Workspace not found: " + workspaceId);
		}
		if (!SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to view it");
		}

		// Read CONFIG_JSON.hooks (empty if missing/unreadable).
		List<Map<String, Object>> hooks = new ArrayList<>();
		JSONObject cfg = null;
		try {
			cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
		} catch (Exception e) {
			classLogger.warn("Failed to load CONFIG_JSON for workspaceId '{}'; returning empty hooks", workspaceId, e);
		}
		if (cfg != null) {
			JSONArray arr = cfg.optJSONArray("hooks");
			if (arr != null) {
				for (int i = 0; i < arr.length(); i++) {
					JSONObject entry = arr.optJSONObject(i);
					if (entry == null) {
						continue;
					}
					hooks.add(jsonObjectToMap(entry));
				}
			}
		}

		// Snapshot of registered hook kinds - drives the FE's "add hook"
		// dropdown without an extra round-trip.
		Set<String> knownKinds = AgentHookRegistry.knownKinds();

		Map<String, Object> response = new LinkedHashMap<>();
		response.put("hooks", hooks);
		response.put("knownKinds", new ArrayList<>(knownKinds));
		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Shallow-convert a JSONObject to a Map<String,Object> so the Pixel MAP
	 * serializer hands the FE a plain JSON tree.
	 */
	private static Map<String, Object> jsonObjectToMap(JSONObject obj) {
		Map<String, Object> m = new HashMap<>();
		for (String key : obj.keySet()) {
			Object val = obj.opt(key);
			if (val instanceof JSONObject) {
				m.put(key, jsonObjectToMap((JSONObject) val));
			} else if (val instanceof JSONArray) {
				m.put(key, jsonArrayToList((JSONArray) val));
			} else {
				m.put(key, val);
			}
		}
		return m;
	}

	private static List<Object> jsonArrayToList(JSONArray arr) {
		List<Object> out = new ArrayList<>(arr.length());
		for (int i = 0; i < arr.length(); i++) {
			Object v = arr.opt(i);
			if (v instanceof JSONObject) {
				out.add(jsonObjectToMap((JSONObject) v));
			} else if (v instanceof JSONArray) {
				out.add(jsonArrayToList((JSONArray) v));
			} else {
				out.add(v);
			}
		}
		return out;
	}
}
