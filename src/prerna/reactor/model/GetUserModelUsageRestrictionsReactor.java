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

import prerna.auth.User;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.utils.ModelUsageLimitResolver;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.model.usage.ModelUsagePathView;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * How usage limits apply to the signed in person on one model: every ceiling
 * and source in order, which one their calls are billed to, and for each rule
 * what is used and when it resets. A person over a limit gets the path with
 * that step reached, not an error. The JSON is {@link ModelUsagePathView}'s.
 */
public class GetUserModelUsageRestrictionsReactor extends AbstractReactor {

	private static final String ENGINE = "engine";

	public GetUserModelUsageRestrictionsReactor() {
		this.keysToGet = new String[] { ENGINE };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = insight.getUser();
		String engineId = this.keyValue.get(ENGINE);

		if (user == null || user.isAnonymous()) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}
		ModelUsageLimitPath path = ModelUsageLimitResolver.resolve(ModelUsageLimitResolver.personOf(user), engineId);
		return new NounMetadata(ModelUsagePathView.toMap(path, true), PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Returns how model usage limits apply to the signed in user on a model: each limit, what is used, "
				+ "when it resets, and which limit calls are billed to.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ENGINE)) {
			return "Engine id of the model whose usage limits to return";
		}
		return super.getDescriptionForKey(key);
	}
}
