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
package prerna.reactor.appprofile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.reactor.appprofile.AppProfileUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Remove a user from a sub-group.
 *
 * <p>Pixel: {@code RemoveAppUserSubgroup(app=["appId"], userId=["userId"], subgroup=["subgroupId"]);}</p>
 */
public class RemoveAppUserSubgroupReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RemoveAppUserSubgroupReactor.class);

	public RemoveAppUserSubgroupReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.USER_ID.getKey(), ReactorKeysEnum.SUBGROUP_ID.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String appId = this.keyValue.get(ReactorKeysEnum.APP.getKey());
		String userId = this.keyValue.get(ReactorKeysEnum.USER_ID.getKey());
		String subgroupId = this.keyValue.get(ReactorKeysEnum.SUBGROUP_ID.getKey());

		if (!AppProfileUtils.canAssignProfiles(user, appId)) {
			throw new IllegalArgumentException("User does not have permission to manage users for this app.");
		}
		AppProfileUtils.removeUserSubgroup(appId, userId, subgroupId);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("User removed from subgroup."));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Remove a user from a sub-group.";
	}
}
