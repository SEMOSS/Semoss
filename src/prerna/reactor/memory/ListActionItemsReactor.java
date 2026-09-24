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
package prerna.reactor.memory;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists memory action items visible to the current user, with optional
 * assignee/status (multi-value)/room/search filters. Same workspace-visibility
 * check as ListMemoriesReactor. Response is paginated: {@code {action_items,
 * total_count, has_more}}.
 */
public class ListActionItemsReactor extends AbstractReactor {

	public ListActionItemsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), ReactorKeysEnum.OWNER.getKey(),
				ReactorKeysEnum.STATUS.getKey(), ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.SEARCH.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public String getReactorDescription() {
		return """
				Lists memory action items (follow-up tasks) visible to the current user, \
				with optional owner/status/room filters and free-text content search.\
				""";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		if (workspaceId != null && !workspaceId.isBlank()
				&& !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}
		List<String> owners = getListStringFromKeyOrCurRow(ReactorKeysEnum.OWNER.getKey());
		List<String> statuses = getListStringFromKeyOrCurRow(ReactorKeysEnum.STATUS.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String search = this.keyValue.get(ReactorKeysEnum.SEARCH.getKey());
		Integer limit = parseIntOrNull(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));
		Integer offset = parseIntOrNull(this.keyValue.get(ReactorKeysEnum.OFFSET.getKey()));

		Map<String, Object> result = MemoryUtils.listActionItems(userId, workspaceId, owners, statuses, roomId, search,
				limit, offset);
		return new NounMetadata(result, PixelDataType.MAP);
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
		if (key.equals(ReactorKeysEnum.WORKSPACE_ID.getKey())) {
			return "Optional workspace to list shared action items from";
		}
		if (key.equals(ReactorKeysEnum.OWNER.getKey())) {
			return "Optional owner/assignee filter (single value or list)";
		}
		if (key.equals(ReactorKeysEnum.STATUS.getKey())) {
			return "Optional status filter (single value or list)";
		}
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "Optional room filter";
		}
		if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "Optional case-insensitive substring to search for in action item content";
		}
		return super.getDescriptionForKey(key);
	}

}
