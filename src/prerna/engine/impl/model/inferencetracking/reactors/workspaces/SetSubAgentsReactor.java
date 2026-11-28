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

import java.util.LinkedHashSet;
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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetSubAgentsReactor extends AbstractWorkspaceReactor {

	private static final Logger classLogger = LogManager.getLogger(SetSubAgentsReactor.class);

	private static final String SUBAGENTS_KEY = "subagents";
	private static final String ALIAS_FIELD = "alias";
	private static final String WORKSPACE_ID_FIELD = "workspaceId";
	private static final String DESCRIPTION_FIELD = "description";

	public SetSubAgentsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), SUBAGENTS_KEY };
		this.keyRequired = new int[] { 1, 1 };
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
		if (!SecurityProjectUtils.userCanEditProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}

		List<Map<String, Object>> rawSubagents = getList(SUBAGENTS_KEY, List.of());
		JSONArray validated = validateAndBuildSubagents(rawSubagents);

		JSONObject cfg;
		try {
			cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
		} catch (Exception e) {
			classLogger.warn("Failed to load existing CONFIG_JSON for workspaceId '{}'; starting fresh", workspaceId,
					e);
			cfg = null;
		}
		if (cfg == null) {
			cfg = new JSONObject();
			cfg.put("schema_version", 1);
		}
		cfg.put(SUBAGENTS_KEY, validated);

		try {
			ModelInferenceLogsUtils.updateWorkspaceConfigJson(workspaceId, cfg);
		} catch (Exception e) {
			classLogger.error("Failed to persist CONFIG_JSON.subagents for workspaceId '{}'.", workspaceId, e);
			throw new IllegalArgumentException("Failed to update workspace subagents: " + e.getMessage(), e);
		}

		return new NounMetadata(cfg.toString(), PixelDataType.CONST_STRING);
	}

	private static JSONArray validateAndBuildSubagents(List<Map<String, Object>> raw) {
		JSONArray out = new JSONArray();
		if (raw == null) {
			return out;
		}
		Set<String> seenAliases = new LinkedHashSet<>();
		for (int i = 0; i < raw.size(); i++) {
			Map<String, Object> entry = raw.get(i);
			if (entry == null) {
				throw new IllegalArgumentException("subagents[" + i + "] is null");
			}
			String alias = trimToNullOrString(entry.get(ALIAS_FIELD));
			String targetWorkspaceId = trimToNullOrString(entry.get(WORKSPACE_ID_FIELD));
			String description = trimToNullOrString(entry.get(DESCRIPTION_FIELD));

			if (alias == null) {
				throw new IllegalArgumentException("subagents[" + i + "] missing required 'alias'");
			}
			if (targetWorkspaceId == null) {
				throw new IllegalArgumentException("subagents[" + i + "] missing required 'workspaceId'");
			}
			if (!seenAliases.add(alias)) {
				throw new IllegalArgumentException(
						"subagents[" + i + "] duplicate alias '" + alias + "' (aliases must be unique)");
			}
			if (ModelInferenceLogsUtils.getWorkspaceEntry(targetWorkspaceId) == null) {
				throw new IllegalArgumentException(
						"subagents[" + i + "] target workspaceId '" + targetWorkspaceId + "' does not exist");
			}

			JSONObject clean = new JSONObject();
			clean.put(ALIAS_FIELD, alias);
			clean.put(WORKSPACE_ID_FIELD, targetWorkspaceId);
			if (description != null) {
				clean.put(DESCRIPTION_FIELD, description);
			}
			out.put(clean);
		}
		return out;
	}

	private static String trimToNullOrString(Object v) {
		if (v == null) {
			return null;
		}
		String s = String.valueOf(v).trim();
		return s.isEmpty() ? null : s;
	}
}
