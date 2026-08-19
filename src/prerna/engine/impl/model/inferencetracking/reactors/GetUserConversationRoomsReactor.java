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
package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserConversationRoomsReactor extends AbstractReactor {
	private static final String INCLUDE_UNNAMED_ROOMS = "includeUnnamedRooms";
	private static final String INCLUDE_CHILD_ROOMS = "includeChildRooms";

	public GetUserConversationRoomsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.SEARCH.getKey(), ReactorKeysEnum.SORT.getKey(),
				ReactorKeysEnum.PINNED.getKey(), "roomOptionsSearch", INCLUDE_UNNAMED_ROOMS, INCLUDE_CHILD_ROOMS };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null) {
			projectId = this.insight.getContextProjectId();
		}
		if (projectId == null) {
			projectId = this.insight.getProjectId();
		}

		long limit = getLong(ReactorKeysEnum.LIMIT.getKey(), -1L);
		long offset = getLong(ReactorKeysEnum.OFFSET.getKey(), -1L);

		// Only accept "asc" or "desc", default to DESC
		String sortDir = this.keyValue.getOrDefault("sort", "DESC");
		sortDir = (sortDir != null) ? sortDir.trim().toUpperCase() : "DESC";
		if (!sortDir.equals("ASC") && !sortDir.equals("DESC")) {
			sortDir = "DESC";
		}

		String search = this.keyValue.get(ReactorKeysEnum.SEARCH.getKey());
		if (search != null && !search.trim().isEmpty()
				&& (projectId == null || projectId.trim().isEmpty())) {
			throw new IllegalArgumentException("A project must be provided or available from the current insight");
		}

		// Optional pinned filter: true/false to filter, null/absent to ignore
		Boolean pinned = null;
		String pinnedStr = this.keyValue.get(ReactorKeysEnum.PINNED.getKey());
		if (pinnedStr != null && !pinnedStr.trim().isEmpty()) {
			pinned = Boolean.parseBoolean(pinnedStr.trim());
		}

		// Optional free-text search against the OPTIONS JSON column.
		String roomOptionsSearch = this.keyValue.get("roomOptionsSearch");
		if (roomOptionsSearch != null) {
			roomOptionsSearch = roomOptionsSearch.trim();
			if (roomOptionsSearch.isEmpty()) {
				roomOptionsSearch = null;
			}
		}
		boolean includeUnnamedRooms = getBoolean(INCLUDE_UNNAMED_ROOMS, false);
		boolean includeChildRooms = getBoolean(INCLUDE_CHILD_ROOMS, false);

		// Call new overload of getUserConversations
		List<Map<String, Object>> output = ModelInferenceLogsUtils.getUserConversations(
				user.getPrimaryLoginToken().getId(), projectId, limit, offset, sortDir, search, pinned,
				roomOptionsSearch, includeUnnamedRooms, includeChildRooms);

		return new NounMetadata(output, PixelDataType.VECTOR);
	}

	@Override
	public String getReactorDescription() {
		return "Retrieves conversation rooms for the current user, with optional project scoping, paging, room-name search, sort direction, and pinned filtering.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "Project ID used to scope rooms. If omitted, the current insight context project is used.";
		} else if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "Maximum number of rooms to return. Use -1 (or omit) to return all available rooms.";
		} else if (ReactorKeysEnum.OFFSET.getKey().equals(key)) {
			return "Number of rooms to skip before returning results.";
		} else if (ReactorKeysEnum.SEARCH.getKey().equals(key)) {
			return "Optional room-name search term (case-insensitive).";
		} else if (ReactorKeysEnum.SORT.getKey().equals(key)) {
			return "Sort direction by room creation date. Accepts ASC or DESC (default is DESC).";
		} else if (ReactorKeysEnum.PINNED.getKey().equals(key)) {
			return "Optional pinned filter: true for pinned rooms only, false for unpinned rooms only, omit for no pinned filter.";
		} else if ("roomOptionsSearch".equals(key)) {
			return "Optional free-text search term applied against the room's options JSON. Any room whose options contain this substring is included.";
		} else if (INCLUDE_UNNAMED_ROOMS.equals(key)) {
			return "Whether to include rooms with a null or empty name. Defaults to false.";
		} else if (INCLUDE_CHILD_ROOMS.equals(key)) {
			return "Whether to include rooms that have a parent room. Defaults to false.";
		}
		return super.getDescriptionForKey(key);
	}
}
