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
				Returns the full settings/metadata record for a single engine (database, model, vector, storage, function, etc.).

				Access: the engine must be one the current user can view, or the engine must be marked discoverable. \
				If neither is true the reactor throws "Engine does not exist or user does not have access to the engine". \
				Use AdminEngineInfo if you need to read an engine without regard to the caller's permissions.

				Inputs:
				  engine   (required) - the engine id.
				  metaKeys (optional) - restrict the returned metadata tags to this list; omit to return all metadata.

				Returns a single map (PROJECT_INFO/CUSTOM_DATA_STRUCTURE) containing:
				  Core engine fields: engine_id, engine_name, engine_display_name, engine_type, engine_subtype,
				    engine_cost, engine_discoverable, engine_global, engine_tool_app, engine_created_by,
				    engine_created_by_type, engine_date_created, low_engine_name, engine_description.
				  Permission fields (relative to the calling user): engine_user_permission, engine_group_permission, engine_favorite.
				  Metadata: one entry per metadata tag (e.g. tag, domain, etc.); a tag with multiple values is returned as a list.
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ENGINE.getKey().equals(key)) {
			return "Id of the engine to look up";
		} else if (ReactorKeysEnum.META_KEYS.getKey().equals(key)) {
			return "Optional list of metadata tag names to return for the engine; omit to return all metadata tags";
		}
		return super.getDescriptionForKey(key);
	}

}
