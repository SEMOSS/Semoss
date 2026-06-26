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
package prerna.auth.utils.reactors.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminEngineInfoReactor extends AbstractReactor {
	
	public AdminEngineInfoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		if(engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);

		List<Map<String, Object>> baseInfo = adminUtils.getAllEngineSettings(Arrays.asList(engineId), null, null, null, null, null);
		if(baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any engine data");
		}
		
		// we filtered to a single database
		Map<String, Object> databaseInfo = baseInfo.get(0);
		databaseInfo.putAll(SecurityEngineUtils.getAggregateEngineMetadata(engineId, getMetaKeys(), true));
		return new NounMetadata(databaseInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);
	}
	
	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.META_KEYS.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		return null;
	}

	@Override
	public String getReactorDescription() {
		return """
				Admin-only: returns the full settings/metadata record for a single engine regardless of the caller's \
				engine permissions. The calling user must be an application admin or the reactor throws \
				"User must be an admin to perform this function".

				This is the admin counterpart to EngineInfo, which instead enforces the caller's view/discoverable access.

				Inputs:
				  engine   (required) - the engine id.
				  metaKeys (optional) - restrict the returned metadata tags to this list; omit to return all metadata.

				Returns a single map (ENGINE_INFO/CUSTOM_DATA_STRUCTURE) containing:
				  Core engine fields: engine_id, engine_name, engine_display_name, engine_type, engine_subtype,
				    engine_cost, engine_discoverable, engine_global, engine_tool_app, engine_created_by,
				    engine_created_by_type, engine_date_created, low_engine_name.
				  Legacy aliases (deprecated, kept for backwards compatibility - prefer the engine_* fields above):
				    database_id, database_name, database_tool_app, database_global, low_database_name,
				    app_id, app_name, app_display_name, app_global, tool_app.
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