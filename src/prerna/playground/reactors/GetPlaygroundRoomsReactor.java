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
package prerna.playground.reactors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.collaboration.CollaborationUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.PlaygroundRoomFilter;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.PlaygroundRoomGroup;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.PlaygroundRoomSort;
import prerna.engine.impl.model.inferencetracking.reactors.GetUserConversationRoomsReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPlaygroundRoomsReactor extends GetUserConversationRoomsReactor {

	private static final String MODE = "mode";
	private static final String SORT_BY = "sortBy";
	private static final String GROUP_BY = "groupBy";
	private static final String ROOM_IDS = "roomIds";
	private static final String SEARCH_MESSAGES = "searchMessages";
	private static final String WORKSPACE_ID = "workspaceId";
	private static final String INCLUDE_UNNAMED_ROOMS = "includeUnnamedRooms";
	private static final String INCLUDE_CHILD_ROOMS = "includeChildRooms";
	private static final String[] PLAYGROUND_KEYS = { MODE, SORT_BY, GROUP_BY, ROOM_IDS, SEARCH_MESSAGES,
			WORKSPACE_ID };

	public GetPlaygroundRoomsReactor() {
		super();
		int inherited = this.keysToGet.length;
		this.keysToGet = Arrays.copyOf(this.keysToGet, inherited + PLAYGROUND_KEYS.length);
		System.arraycopy(PLAYGROUND_KEYS, 0, this.keysToGet, inherited, PLAYGROUND_KEYS.length);
		this.keyRequired = Arrays.copyOf(this.keyRequired, this.keysToGet.length);
	}

	@Override
	public NounMetadata execute() {
		GenRowStruct projectGRS = this.store.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey());
		if (projectGRS != null) {
			projectGRS.clear();
		} else {
			projectGRS = new GenRowStruct();
		}
		// mode picks the system project; null lists normal playground rooms
		GenRowStruct modeGRS = this.store.getNoun(MODE);
		String mode = modeGRS == null || modeGRS.isEmpty() ? null : String.valueOf(modeGRS.get(0));
		projectGRS.add(new NounMetadata(CollaborationUtils.projectIdForMode(mode), PixelDataType.CONST_STRING));
		this.store.addNoun(ReactorKeysEnum.PROJECT.getKey(), projectGRS);

		// without the playground list keys, list exactly as GetUserConversationRooms does
		if (isEmpty(SORT_BY) && isEmpty(GROUP_BY) && isEmpty(ROOM_IDS) && isEmpty(SEARCH_MESSAGES)
				&& isEmpty(WORKSPACE_ID)) {
			return super.execute();
		}
		return listPlaygroundRooms();
	}

	/**
	 * Lists rooms grouped, pinned first and sorted, with each room's agent name and
	 * pending action count.
	 */
	private NounMetadata listPlaygroundRooms() {
		organizeKeys();
		User user = requireUser();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());

		PlaygroundRoomSort sortBy = isEmpty(SORT_BY) ? PlaygroundRoomSort.DATE_CREATED
				: PlaygroundRoomSort.fromKey(this.keyValue.get(SORT_BY));
		PlaygroundRoomGroup groupBy = isEmpty(GROUP_BY) ? PlaygroundRoomGroup.NONE
				: PlaygroundRoomGroup.fromKey(this.keyValue.get(GROUP_BY));
		String sortDir = this.keyValue.get(ReactorKeysEnum.SORT.getKey());

		Boolean pinned = null;
		String pinnedStr = this.keyValue.get(ReactorKeysEnum.PINNED.getKey());
		if (pinnedStr != null && !pinnedStr.trim().isEmpty()) {
			pinned = Boolean.parseBoolean(pinnedStr.trim());
		}

		List<String> roomIds = null;
		GenRowStruct roomIdsGRS = this.store.getNoun(ROOM_IDS);
		if (roomIdsGRS != null && !roomIdsGRS.isEmpty()) {
			roomIds = roomIdsGRS.getAllStrValues();
		}

		String workspaceId = this.keyValue.get(WORKSPACE_ID);
		if (workspaceId != null && workspaceId.trim().isEmpty()) {
			workspaceId = null;
		}

		PlaygroundRoomFilter filter = new PlaygroundRoomFilter(this.keyValue.get(ReactorKeysEnum.SEARCH.getKey()),
				getBoolean(SEARCH_MESSAGES, false), pinned, roomIds, workspaceId,
				getBoolean(INCLUDE_UNNAMED_ROOMS, false), getBoolean(INCLUDE_CHILD_ROOMS, false));
		List<Map<String, Object>> output = ModelInferenceLogsUtils.getPlaygroundRooms(
				user.getPrimaryLoginToken().getId(), projectId, getLong(ReactorKeysEnum.LIMIT.getKey(), -1L),
				getLong(ReactorKeysEnum.OFFSET.getKey(), -1L), sortBy, sortDir, groupBy, filter);
		return new NounMetadata(output, PixelDataType.VECTOR);
	}

	private boolean isEmpty(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		return grs == null || grs.isEmpty();
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Retrieves the current user's playground rooms. With sortBy, groupBy, roomIds, searchMessages or workspaceId, rooms are ordered by group, then pinned first, then the sort column, and each room includes DATE_UPDATED and WORKSPACE_NAME (and NAME_MATCH when searching).")
				.key(MODE, "Optional mode; collaboration lists collaboration rooms instead of playground rooms.")
				.key(SORT_BY,
						"Optional sort column: dateCreated (default), dateUpdated (last activity) or name. The sort key gives the direction.")
				.key(GROUP_BY,
						"Optional grouping applied before pinned rooms and the sort: none (default) or agent (by workspace name, rooms without an agent last).")
				.key(ROOM_IDS, "Optional list of room ids (at most "
						+ ModelInferenceLogsUtils.MAX_PLAYGROUND_ROOM_IDS + ") to limit the list to.")
				.key(SEARCH_MESSAGES,
						"Whether search also matches the text of the room's messages, not only its name. Rooms whose name matches come first. Defaults to false.")
				.key(WORKSPACE_ID, "Optional agent (workspace) id; only rooms started with that agent are listed.");
	}

}
