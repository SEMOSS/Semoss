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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.ModelUsageRestrictionUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserModelUsageRestrictionsReactor extends AbstractReactor {

	public GetUserModelUsageRestrictionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = insight.getUser();

		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		List<String> engineIds = getList(ReactorKeysEnum.ENGINE.getKey());
		boolean explicitEngines = engineIds != null && !engineIds.isEmpty();

		if (!explicitEngines) {
			engineIds = SecurityEngineUtils.getUserEngineIdList(user,
					List.of(IEngine.CATALOG_TYPE.MODEL.name()), true, true, true);
		} else {
			for (String engineId : engineIds) {
				if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
					throw new IllegalArgumentException(
							"Model " + engineId + " does not exist or user does not have access to this model");
				}
			}
		}

		Map<Object, Object> idToAlias = SecurityEngineUtils.getEngineAliasForIds(engineIds);

		List<Map<String, Object>> result = new ArrayList<>();
		for (String engineId : engineIds) {
			Map<String, Object> restrictionMap = ModelUsageRestrictionUtility.getModelUsageRestriction(user, engineId);
			restrictionMap.put("ENGINE_ID", engineId);
			restrictionMap.put("ENGINE_NAME", idToAlias.get(engineId));
			result.add(restrictionMap);
		}

		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	@Override
	public String getReactorDescription() {
		return "Returns model usage restrictions for the current user. Accepts a single engine ID, a list of engine IDs, or omit to return restrictions for all accessible models.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Optional engine ID or list of engine IDs to check restrictions for. Omit to return restrictions for all accessible models.";
		}
		return super.getDescriptionForKey(key);
	}
}
