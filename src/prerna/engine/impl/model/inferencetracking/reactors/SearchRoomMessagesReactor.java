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

	public SearchRoomMessagesReactor() {
		// this expects projectId and search term
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.SEARCH.getKey(), };
		this.keyRequired = new int[] { 0, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// Get user
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		// Get projectId
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null) {
			projectId = this.insight.getContextProjectId();
		}

		// Get keyword
		String keyword = this.keyValue.get(this.keysToGet[1]);
		if (keyword == null || keyword.trim().isEmpty()) {
			throw new IllegalArgumentException("Search keyword must be provided");
		}

		// Query messages
		List<Map<String, Object>> results;
		try {
			results = ModelInferenceLogsUtils.searchMessages(userId, projectId, keyword);
		} catch (Exception e) {
			classLogger.error("Error searching room messages", e);
			throw new RuntimeException("Could not search room messages: " + e.getMessage(), e);
		}

		// Return results as VECTOR
		return new NounMetadata(results, PixelDataType.VECTOR);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor searches through the messages in the user's conversation rooms for a given keyword within a specified project. "
				+ "It returns the matching messages, including relevant details such as room ID, message text, and message ID, for rooms the user has access to.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The project ID for which to search room messages. If no project ID is passed, then all rooms for the user will be searched.";
		} else if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "The search term to use to search for within the messages. All messages containing this text (case-insensitive) will be returned.";
		}
		return super.getDescriptionForKey(key);
	}
}