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

import prerna.auth.User;
import prerna.auth.utils.AppFeatureFlagUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates a new feature flag for an app.
 *
 * Pixel: CreateAppFeatureFlag(app="myApp", key="new-dashboard",
 * description="Redesigned UI")
 *
 * Requires app owner or admin.
 */
public class CreateAppFeatureFlagReactor extends AbstractReactor {

	private static final String APP_KEY = "app";
	private static final String FLAG_KEY_PARAM = "key";
	private static final String DESCRIPTION_KEY = "description";

	public CreateAppFeatureFlagReactor() {
		this.keysToGet = new String[] { APP_KEY, FLAG_KEY_PARAM, DESCRIPTION_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		String appId = this.keyValue.get(APP_KEY);
		String flagKey = this.keyValue.get(FLAG_KEY_PARAM);
		String description = this.keyValue.get(DESCRIPTION_KEY);

		if (appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("'app' is required");
		}
		if (flagKey == null || flagKey.isEmpty()) {
			throw new IllegalArgumentException("'key' is required");
		}
		if (!AppFeatureFlagUtils.canManageFlags(user, appId)) {
			throw new IllegalArgumentException("User does not have permission to manage feature flags for this app");
		}

		String createdBy = AppFeatureFlagUtils.getUserId(user);
		String flagId = AppFeatureFlagUtils.createFlag(appId, flagKey, description, createdBy);

		return new NounMetadata(flagId, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
