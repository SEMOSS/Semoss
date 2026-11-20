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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Admin only: every engine and project a user can use, with the user's
 * effective permission on each and whether it comes from their own grant, a
 * custom team's grant, or the item being public.
 */
public class AdminGetUserCatalogAccessReactor extends AbstractReactor {

	private static final String USER_ID = "userId";

	public AdminGetUserCatalogAccessReactor() {
		this.keysToGet = new String[] { USER_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User is not an admin and does not have access. Please login as an admin");
		}
		String userId = this.keyValue.get(USER_ID);
		if (userId == null || userId.isBlank()) {
			throw new IllegalArgumentException("Must provide the user id");
		}
		List<Map<String, Object>> access = SecurityAdminUtils.getUserCatalogAccess(userId.trim());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("userId", userId.trim());
		result.put("access", access);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Admin Only: Every engine and project a user can use, each with id, kind (ENGINE or "
						+ "PROJECT), the user's effective permission (1 owner, 2 edit, 3 read only) and its source: "
						+ "DIRECT for the user's own grant, TEAM for a custom team's grant, PUBLIC for a public item. "
						+ "The best permission wins; a tie goes to DIRECT, then TEAM, then PUBLIC.")
				.key(USER_ID, "The user's id")
				.output(Map.class, "userId and access");
	}
}
