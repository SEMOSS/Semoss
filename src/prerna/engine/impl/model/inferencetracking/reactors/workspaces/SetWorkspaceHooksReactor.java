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

import java.util.List;
import java.util.Map;

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
 * Reactor that sets the hook list on a workspace, persisting to
 * {@code WORKSPACE.CONFIG_JSON.hooks[]}.
 *
 * <p>Input keys:
 * <ul>
 *   <li>{@code workspaceId} — required</li>
 *   <li>{@code hooks} — required, list of maps. Each entry must have a
 *       {@code kind} known to {@link AgentHookRegistry}; other keys are
 *       persisted as-is but ignored by the loader today.</li>
 * </ul>
 *
 * <p>Validation rejects unknown {@code kind} values at write time so the run-time
 * loader doesn't silently drop them. To add a new hook: implement
 * {@link prerna.reactor.agent.IMessageHook} and register it with
 * {@link AgentHookRegistry#register(String, java.util.function.Supplier)}.
 *
 * <p>Returns the new full {@code CONFIG_JSON} as a {@link PixelDataType#CONST_STRING}
 * for FE display.
 */
public class SetWorkspaceHooksReactor extends AbstractWorkspaceReactor {

    private static final Logger classLogger = LogManager.getLogger(SetWorkspaceHooksReactor.class);

    private static final String HOOKS_KEY = "hooks";

    public SetWorkspaceHooksReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), HOOKS_KEY };
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

        List<Map<String, Object>> rawHooks = getList(HOOKS_KEY, List.of());
        JSONArray validated = validateAndBuildHooks(rawHooks);

        // Merge into existing CONFIG_JSON (preserve other fields), then write back.
        JSONObject cfg;
        try {
            cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
        } catch (Exception e) {
            classLogger.warn("Failed to load existing CONFIG_JSON for workspaceId '{}'; starting fresh", workspaceId, e);
            cfg = null;
        }
        if (cfg == null) {
            cfg = new JSONObject();
            cfg.put("schema_version", 1);
        }
        cfg.put("hooks", validated);

        try {
            ModelInferenceLogsUtils.updateWorkspaceConfigJson(workspaceId, cfg);
        } catch (Exception e) {
            classLogger.error("Failed to persist CONFIG_JSON.hooks for workspaceId '{}'.", workspaceId, e);
            throw new IllegalArgumentException("Failed to update workspace hooks: " + e.getMessage(), e);
        }

        return new NounMetadata(cfg.toString(), PixelDataType.CONST_STRING);
    }

    /**
     * Validates each entry has a known {@code kind} and rebuilds the JSON array
     * canonically. Throws on the first unknown kind so the FE gets a clear
     * error rather than silent drop at read time.
     */
    private static JSONArray validateAndBuildHooks(List<Map<String, Object>> raw) {
        JSONArray out = new JSONArray();
        if (raw == null) {
            return out;
        }
        for (int i = 0; i < raw.size(); i++) {
            Map<String, Object> entry = raw.get(i);
            if (entry == null) {
                throw new IllegalArgumentException("hooks[" + i + "] is null");
            }
            Object kindObj = entry.get("kind");
            if (kindObj == null) {
                throw new IllegalArgumentException("hooks[" + i + "] missing required 'kind'");
            }
            String kind = String.valueOf(kindObj);
            if (!AgentHookRegistry.isKnown(kind)) {
                throw new IllegalArgumentException("hooks[" + i + "] unknown kind '" + kind
                        + "'. Known kinds: " + AgentHookRegistry.knownKinds());
            }

            // Kind-specific validation. The loader's hook.configure(spec)
            // also validates at run time, but we catch obvious errors here
            // so the FE gets a synchronous error rather than discovering
            // the misconfiguration on the next agent run.
            if (AgentHookRegistry.PIXEL.equals(kind)) {
                Object pixelObj = entry.get("pixel");
                if (pixelObj == null || String.valueOf(pixelObj).trim().isEmpty()) {
                    throw new IllegalArgumentException(
                            "hooks[" + i + "] kind='pixel' requires a non-empty 'pixel' field");
                }
            }

            // Persist the whole entry as-is so kind-specific fields
            // (pixel, events, params, future additions) round-trip
            // through the config and reach the loader's configure() call.
            out.put(new JSONObject(entry));
        }
        return out;
    }
}
