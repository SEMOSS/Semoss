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

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Admin-only cross-user/cross-workspace memory listing, mirroring
 * {@link AdminGetLlmFeedbackReactor}'s permission check.
 */
public class AdminListAllMemoriesReactor extends AbstractReactor {

	public AdminListAllMemoriesReactor() {
		this.keysToGet = new String[] { "userId", ReactorKeysEnum.WORKSPACE_ID.getKey(),
				ReactorKeysEnum.EVENT_TYPE.getKey(), ReactorKeysEnum.START_DATE.getKey(),
				ReactorKeysEnum.END_DATE.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey() };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		organizeKeys();

		String userIdFilter = this.keyValue.get("userId");
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		String eventType = this.keyValue.get(ReactorKeysEnum.EVENT_TYPE.getKey());
		String startDate = this.keyValue.get(ReactorKeysEnum.START_DATE.getKey());
		String endDate = this.keyValue.get(ReactorKeysEnum.END_DATE.getKey());
		Integer limit = parseIntOrNull(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));
		Integer offset = parseIntOrNull(this.keyValue.get(ReactorKeysEnum.OFFSET.getKey()));

		List<Map<String, Object>> memories = MemoryUtils.adminListMemories(userIdFilter, workspaceId, eventType,
				startDate, endDate, limit, offset);
		return new NounMetadata(memories, PixelDataType.VECTOR);
	}

	private Integer parseIntOrNull(String value) {
		if (value == null || value.isBlank()) {
			return null;
		}
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid numeric value: " + value);
		}
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("userId")) {
			return "Optional user id to filter memories by";
		}
		return super.getDescriptionForKey(key);
	}

}
