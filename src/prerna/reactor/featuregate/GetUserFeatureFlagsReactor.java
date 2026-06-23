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
package prerna.reactor.featuregate;

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AppFeatureFlagUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns all feature flag keys and their evaluated state details for the
 * current user.
 * Each flag includes whether it is enabled and the version data used to
 * evaluate it.
 *
 * Pixel: GetUserFeatureFlags(app="myApp")
 *
 * Any authenticated user with access to the app may call this.
 */
public class GetUserFeatureFlagsReactor extends AbstractReactor {

	private static final String APP_KEY = "app";

	public GetUserFeatureFlagsReactor() {
		this.keysToGet = new String[] { APP_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		String appId = this.keyValue.get(APP_KEY);
		if (appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("'app' is required");
		}

		if (!AppFeatureFlagUtils.canEvaluateFlags(user, appId)) {
			throw new IllegalArgumentException("User does not have access to this app");
		}

		Map<String, Map<String, Object>> flags = AppFeatureFlagUtils.getUserFeatureFlagDetails(appId, user);
		return new NounMetadata(flags, PixelDataType.MAP);
	}
}
