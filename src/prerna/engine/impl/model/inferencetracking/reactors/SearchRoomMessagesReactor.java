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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SearchRoomMessagesReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(SearchRoomMessagesReactor.class);

	private static final String INCLUDE_MESSAGE_TEXT = "includeMessageText";

	public SearchRoomMessagesReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.SEARCH.getKey(),
				ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(),
				INCLUDE_MESSAGE_TEXT
		};
		this.keyRequired = new int[] { 0, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null) {
			projectId = this.insight.getContextProjectId();
		}

		String keyword = this.keyValue.get(this.keysToGet[1]);
		if (keyword == null || keyword.trim().isEmpty()) {
			throw new IllegalArgumentException("Search keyword must be provided");
		}

		long limit = getLong(ReactorKeysEnum.LIMIT.getKey(), -1L);
		long offset = getLong(ReactorKeysEnum.OFFSET.getKey(), -1L);
		boolean includeMessageText = getBoolean(INCLUDE_MESSAGE_TEXT, true);

		List<Map<String, Object>> results;
		try {
			results = ModelInferenceLogsUtils.searchMessages(userId, projectId, keyword, limit, offset, includeMessageText);
		} catch (Exception e) {
			classLogger.error("Error searching room messages", e);
			throw new RuntimeException("Could not search room messages: " + e.getMessage(), e);
		}

		return new NounMetadata(results, PixelDataType.VECTOR);
	}

	@Override
	public String getReactorDescription() {
		return "Searches through the messages in the user's conversation rooms for a given keyword. "
				+ "Returns one row per matching room (its most recent matching message): room_id, message_id, "
				+ "room_name, and date_created. "
				+ "Case-insensitive matching is handled by the query framework's ?like comparator. "
				+ "Optionally returns message_text when includeMessageText=true (default). "
				+ "Falls back to the current insight's project when projectId is omitted. "
				+ "Supports limit and offset for pagination over matching rooms.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "Optional project ID to scope the search. Defaults to the current insight's project when omitted.";
		} else if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "The keyword to search for within message content (case-insensitive).";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Maximum number of results to return. Defaults to no cap when omitted.";
		} else if (key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return "Number of results to skip for pagination.";
		} else if (key.equals(INCLUDE_MESSAGE_TEXT)) {
			return "Whether to include message_text in results. Defaults to true.";
		}
		return super.getDescriptionForKey(key);
	}
}
