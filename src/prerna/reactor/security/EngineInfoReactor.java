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
package prerna.reactor.security;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EngineInfoReactor extends AbstractReactor {

	public EngineInfoReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.META_KEYS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);

		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}

		List<Map<String, Object>> baseInfo = null;
		// make sure valid id for user
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		if (SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			// user has access!
			baseInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(), engineId, null);
		} else if (SecurityEngineUtils.engineIsDiscoverable(engineId)) {
			baseInfo = SecurityEngineUtils.getDiscoverableEngineList(engineId, null);
		} else {
			// you dont have access
			throw new IllegalArgumentException("Engine does not exist or user does not have access to the engine");
		}

		if (baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any engine data");
		}

		// we filtered to a single database
		Map<String, Object> databaseInfo = baseInfo.get(0);
		@SuppressWarnings("unchecked")
		List<String> metaKeys = getList(ReactorKeysEnum.META_KEYS.getKey());
		if (metaKeys != null && metaKeys.isEmpty()) {
			metaKeys = null;
		}
		databaseInfo.putAll(SecurityEngineUtils.getAggregateEngineMetadata(engineId, metaKeys, true));
		return new NounMetadata(databaseInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);
	}

	@Override
	public String getReactorDescription() {
		return """
				Returns information for a single engine when the user can access it or it is discoverable.

				Inputs: engine, metaKeys.
				Response keys: prefer engine_* fields (engine_id, engine_name, engine_display_name, engine_type, engine_subtype, engine_cost, engine_discoverable, engine_global, engine_tool_app, engine_created_by, engine_created_by_type, engine_date_created, low_engine_name).
				Any response key prefixed with app_* or database_* is legacy and should not be used.
				""";
	}

}
