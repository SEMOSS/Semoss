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

import java.io.IOException;
import java.util.Map;

import com.google.gson.GsonBuilder;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IModelRouterEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Rewrites a MODEL_ROUTER engine's router.json from the settings UI. The
 * config is validated with the same rules engine open uses before anything is
 * written, and the live engine instance applies the new routing immediately -
 * no engine reload required.
 */
public class UpdateModelRouterConfigReactor extends AbstractReactor {

	public UpdateModelRouterConfigReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have edit access to it");
		}

		IModelEngine model = Utility.getModel(engineId);
		if (!(model instanceof IModelRouterEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a model router");
		}

		Map<String, Object> config = this.<String, Object>getGenericMap(ReactorKeysEnum.MAP.getKey(), null);
		if (config == null || config.isEmpty()) {
			throw new IllegalArgumentException("Must provide the routing configuration map");
		}

		String json = new GsonBuilder().disableHtmlEscaping().create().toJson(config);
		try {
			// validates first and writes nothing when validation fails
			((IModelRouterEngine) model).updateConfig(json);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to write the router configuration: " + e.getMessage(), e);
		}

		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pushEngine(engineId);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Validates and saves the routing configuration (router.json) for a model router engine, applying it to the running engine immediately. Requires edit access to the engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the model router engine";
		}
		if (key.equals(ReactorKeysEnum.MAP.getKey())) {
			return "The routing configuration as a map matching the router.json schema";
		}
		return super.getDescriptionForKey(key);
	}
}
