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

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IModelRouterEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Returns the raw router.json contents for a MODEL_ROUTER engine so the
 * settings UI can load the current routing configuration. Requires edit
 * access - the config exposes the engine ids of every routing target.
 */
public class GetModelRouterConfigReactor extends AbstractReactor {

	public GetModelRouterConfigReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
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

		try {
			String configJson = ((IModelRouterEngine) model).readConfigJson();
			return new NounMetadata(configJson, PixelDataType.CONST_STRING);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to read the router configuration: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns the routing configuration (router.json contents) for a model router engine. Requires edit access to the engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the model router engine";
		}
		return super.getDescriptionForKey(key);
	}
}
