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
package prerna.reactor.appprofile.subgroup;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.reactor.appprofile.AppProfileUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Bulk-assigns one or more users to a sub-group. Users already in the sub-group are
 * silently skipped; unknown users are returned in the {@code errors} bucket.
 *
 * <p>Pixel: {@code AssignAppUserSubgroup(app=["appId"], userId=["user1", "user2"], subgroup=["subgroupId"]);}</p>
 *
 * <p>Returns a map with three keys: {@code assigned}, {@code skipped}, {@code errors}.</p>
 */
public class AssignAppUserSubgroupReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AssignAppUserSubgroupReactor.class);

	public AssignAppUserSubgroupReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.USER_ID.getKey(), ReactorKeysEnum.SUBGROUP_ID.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String appId = this.keyValue.get(ReactorKeysEnum.APP.getKey());
		String subgroupId = this.keyValue.get(ReactorKeysEnum.SUBGROUP_ID.getKey());
		List<String> userIds = getUserIds();

		if (!AppProfileUtils.canAssignProfiles(user, appId)) {
			throw new IllegalArgumentException("User does not have permission to assign users for this app.");
		}

		Map<String, Object> result = AppProfileUtils.assignUsersToSubgroup(appId, userIds, subgroupId, user);
		classLogger.debug("AssignAppUserSubgroup: assigned={}, skipped={}, errors={}",
				((List<?>) result.get("assigned")).size(),
				((List<?>) result.get("skipped")).size(),
				((Map<?, ?>) result.get("errors")).size());

		NounMetadata noun = new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(
				"Bulk assign complete: " + ((List<?>) result.get("assigned")).size() + " assigned, "
				+ ((List<?>) result.get("skipped")).size() + " skipped, "
				+ ((Map<?, ?>) result.get("errors")).size() + " errors."));
		return noun;
	}

	/**
	 * Reads the {@code userId} parameter as a list. Validates that the list is non-empty
	 * and that every item is a non-blank string.
	 */
	private List<String> getUserIds() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.USER_ID.getKey());
		if (grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("userId must be provided and must contain at least one value.");
		}
		List<String> userIds = grs.getAllStrValues();
		if (userIds == null || userIds.isEmpty()) {
			throw new IllegalArgumentException("userId must contain at least one value.");
		}
		for (String id : userIds) {
			if (id == null || id.trim().isEmpty()) {
				throw new IllegalArgumentException("userId list contains a blank entry — all userId values must be non-blank.");
			}
		}
		return userIds;
	}

	@Override
	public String getReactorDescription() {
		return "Bulk-assign one or more users to a sub-group. Returns assigned, skipped, and errors buckets.";
	}
}
