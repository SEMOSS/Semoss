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

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.AssetUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Rewrites a MODEL_ROUTER engine's router.json from the settings UI. The config is validated with the same rules
 * engine open uses before anything is written, and the live engine applies the new routing immediately.
 *
 * <p>
 * A config in {@code subtask} mode is also versioned: committed to the git repository of the engine's
 * {@code version} folder and recorded as an {@code ENGINE_UPDATE} audit event (config sha-256 and version), both
 * best effort. {@link #save} is the one write path of the learning feature; this pixel is refused to an agent run
 * for a router with learning on, so an agent cannot skip the guardrails of {@link ApplyRouterCandidateReactor}.
 */
public class UpdateModelRouterConfigReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateModelRouterConfigReactor.class);

	public UpdateModelRouterConfigReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		IModelRouterEngine model = editableRouter(this.insight, engineId);
		Map<String, Object> config = this.<String, Object>getGenericMap(ReactorKeysEnum.MAP.getKey(), null);
		if (config == null || config.isEmpty()) {
			throw new IllegalArgumentException("Must provide the routing configuration map");
		}
		var gson = new GsonBuilder().disableHtmlEscaping().create();
		JsonObject proposed = gson.toJsonTree(config).getAsJsonObject();
		var lock = RouterVersions.lock(engineId);
		lock.lock();
		try {
			String current = readConfig(model);
			if (this.insight.getRoomId() != null && ModelRouterEngine.parseAndValidateConfig(current, "router", engineId)
					.improvement() instanceof ModelRouterEngine.Improvement improvement && Boolean.TRUE.equals(improvement.enabled)) {
				throw new SecurityException("An agent run cannot edit a router with learning on; "
						+ "write a candidate with WriteRouterCandidate and apply it with ApplyRouterCandidate");
			}
			preserveManagedLearningSettings(JsonParser.parseString(current).getAsJsonObject(), proposed);
			save(this.insight, engineId, model, gson.toJson(proposed), null);
			return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
		} finally {
			lock.unlock();
		}
	}

	static void preserveManagedLearningSettings(JsonObject current, JsonObject proposed) {
		String key = ModelRouterEngine.Improvement.SCHEDULER_JOB_ID;
		JsonElement live = current.get("improvement");
		JsonElement next = proposed.get("improvement");
		JsonObject liveSettings = live instanceof JsonObject settings ? settings : new JsonObject();
		if (next != null && next.isJsonObject()) {
			JsonObject settings = next.getAsJsonObject();
			for (String managed : List.of(key, "replay")) {
				JsonElement value = liveSettings.get(managed);
				if (settings.has(managed) && !Objects.equals(settings.get(managed), value)) {
					throw new IllegalArgumentException("Only SetRouterLearning can change improvement." + managed);
				}
				if (value != null) {
					settings.add(managed, value);
				}
			}
		} else if (liveSettings.has(key) || liveSettings.has("replay")) {
			if (next != null) {
				throw new IllegalArgumentException("Disable learning before clearing its settings");
			}
			proposed.add("improvement", live);
		}
	}

	/** The router the caller may edit; throws when there is none. */
	static IModelRouterEngine editableRouter(Insight insight, String engineId) {
		if (engineId == null || !SecurityEngineUtils.userCanEditEngine(insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have edit access to it");
		}
		if (!(Utility.getModel(engineId) instanceof IModelRouterEngine router)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a model router");
		}
		return router;
	}

	static String readConfig(IModelRouterEngine router) {
		try {
			return router.readConfigJson();
		} catch (IOException e) {
			throw new IllegalStateException("Unable to read the router configuration: " + e.getMessage(), e);
		}
	}

	/**
	 * The one path every learning-related router write takes: validates and applies the config (nothing is written
	 * when it is invalid), versions it in git with an audit event, and pushes the engine in a cluster. {@code note},
	 * when not null, is added to the commit message and the audit event.
	 */
	static void save(Insight insight, String engineId, IModelRouterEngine model, String json, String note) {
		java.util.concurrent.locks.ReentrantLock lock = RouterVersions.lock(engineId);
		lock.lock();
		try {
			try {
				model.updateConfig(json);
			} catch (IOException e) {
				throw new IllegalStateException("Unable to write the router configuration: " + e.getMessage(), e);
			}
			JsonObject config = JsonParser.parseString(json).getAsJsonObject();
			if (config.has("mode") && "subtask".equalsIgnoreCase(config.get("mode").getAsString().trim())) {
				recordVersion(insight.getUser(), engineId, model, json, config.has("version") ? config.get("version").getAsString() : null, note);
			}
			if (ClusterUtil.IS_CLUSTER) {
				ClusterUtil.pushEngine(engineId);
			}
		} finally {
			lock.unlock();
		}
	}

	/** The engine's version folder, whose git repository holds its assets. */
	static String versionFolder(String engineId) {
		return EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.MODEL, engineId,
				SecurityEngineUtils.getEngineAliasForId(engineId));
	}

	/** The router config's path relative to the engine's version folder. */
	static String configPath(IModelRouterEngine model) {
		String fileName = model.getSmssProp() == null ? null : model.getSmssProp().getProperty(ModelRouterEngine.ROUTER_CONFIG);
		return "assets/" + (fileName == null || fileName.isBlank() ? ModelRouterEngine.DEFAULT_CONFIG_FILE : fileName.trim());
	}

	private static void recordVersion(User user, String engineId, IModelRouterEngine model, String json, Object version, String note) {
		String sha256 = ModelRouterEngine.sha256(json);
		boolean committed = false;
		try {
			String folder = versionFolder(engineId);
			if (!AssetUtility.isGit(folder)) {
				GitRepoUtils.init(folder);
			}
			GitRepoUtils.addSpecificFiles(folder, List.of(configPath(model)));
			AccessToken token = user == null ? null : user.getPrimaryLoginToken();
			GitRepoUtils.commitAddedFiles(folder, "Router config " + version + " sha256 " + sha256.substring(0, 12)
					+ (note == null ? "" : "\n\n" + note), token == null ? null : token.getResolvedUsername(),
					token == null ? null : token.getEmail());
			committed = new File(folder, ".git").exists();
		} catch (Exception e) {
			classLogger.warn("Could not commit the router config of engine {}: {}", engineId, e.getMessage());
		}
		try {
			Map<String, Object> details = new LinkedHashMap<>();
			details.put("reactor", "UpdateModelRouterConfig");
			details.put("field", "routerConfig");
			details.put("version", version);
			details.put("sha256", sha256);
			details.put("committed", committed);
			if (note != null) {
				details.put("note", note);
			}
			UserAuditTrailUtils.recordEngineLifecycle(user, AuditEventType.ENGINE_UPDATE, AuditTargetType.MODEL, engineId,
					SecurityEngineUtils.getEngineDisplayNameForId(engineId), details);
		} catch (Exception e) {
			classLogger.warn("Could not audit the router config update of engine {}: {}", engineId, e.getMessage());
		}
	}

}
