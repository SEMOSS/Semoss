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

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
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
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		Map<String, Object> userRestrictionMap;
		if (engineId != null && !engineId.trim().isEmpty()) {
			if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
				throw new IllegalArgumentException(
						"Model " + engineId + " does not exist or user does not have access to this model");
			}
			userRestrictionMap = ModelUsageRestrictionUtility.getModelUsageRestriction(user, engineId);
		} else {
			userRestrictionMap = ModelUsageRestrictionUtility.getUserLevelRestriction(user);
		}

		return new NounMetadata(userRestrictionMap, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Returns model usage restrictions for a user.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Engine id for which to check model usage restrictions";
		}
		return super.getDescriptionForKey(key);
	}
}