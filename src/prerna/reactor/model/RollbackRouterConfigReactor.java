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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IModelRouterEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ValueUtils;

/**
 * Restores a router's router.json to a committed version as a new commit through
 * {@link UpdateModelRouterConfigReactor#save} and pauses learning, like the regression watch. Needs edit access;
 * an unknown version and the live version are refused, and so is an agent run (a rollback is a person's decision).
 * Rolling back to the version the live one was rolled back from resumes learning.
 */
public class RollbackRouterConfigReactor extends AbstractReactor {

	private static final String VERSION = "version";

	public RollbackRouterConfigReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), VERSION };
	}

	@Override
	public NounMetadata execute() {
		if (this.insight.getRoomId() != null) {
			throw new SecurityException("An agent run cannot roll back a router");
		}
		organizeKeys();
		String engineId = ValueUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
		String version = ValueUtils.trimToNull(this.keyValue.get(VERSION));
		IModelRouterEngine model = UpdateModelRouterConfigReactor.editableRouter(this.insight, engineId);
		if (version == null) {
			throw new IllegalArgumentException("version is required");
		}
		ReentrantLock lock = RouterVersions.lock(engineId);
		lock.lock();
		try {
			String liveJson = UpdateModelRouterConfigReactor.readConfig(model);
			List<RouterVersions.Version> history = RouterVersions.history(UpdateModelRouterConfigReactor.versionFolder(engineId), UpdateModelRouterConfigReactor.configPath(model));
			RouterVersions.Version target = history.stream().filter(v -> v.version().equals(version)).findFirst()
					.orElseThrow(() -> new IllegalArgumentException("Router " + engineId + " has no committed version " + version));
			String live = JsonParser.parseString(liveJson).getAsJsonObject().get("version").getAsString();
			if (version.equals(live)) {
				throw new IllegalArgumentException("Version " + version + " is already live");
			}
			// rolling back to the version this one was rolled back from, while learning is still paused by that rollback, is a roll-forward: learning resumes
			JsonObject improvement = JsonParser.parseString(liveJson).getAsJsonObject().getAsJsonObject("improvement");
			boolean paused = improvement != null && improvement.has("paused") && improvement.get("paused").getAsBoolean();
			boolean pause = !(paused && history.stream().anyMatch(v -> v.version().equals(live) && v.message().contains("rolled back " + version + " to " + live)));
			RouterVersions.restore(this.insight, engineId, model, liveJson, target.json(), "RollbackRouterConfig: rolled back " + live + " to " + version + " by request", pause);
			Map<String, Object> result = new LinkedHashMap<>();
			result.put("status", "rolled_back");
			result.put("version", version);
			result.put("paused", pause);
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Could not roll back the router: " + e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Restores a committed router configuration by operator request")
				.key(ReactorKeysEnum.ENGINE, "The id of the model router engine")
				.key(VERSION, "The committed router version to restore")
				.output(Map.class, "The restored version and whether learning is paused");
	}
}
