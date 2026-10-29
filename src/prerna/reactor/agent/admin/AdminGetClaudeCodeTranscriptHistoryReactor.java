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
package prerna.reactor.agent.admin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.ClaudeCodeTranscriptLocator;
import prerna.reactor.agent.ClaudeCodeTranscriptParser;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads any user's room's Claude Code JSONL transcript from disk, for the admin
 * Agent Runs page, and returns each line parsed through
 * {@link ClaudeCodeTranscriptParser}, as {@code GetClaudeCodeTranscriptHistory}
 * does for the room's owner. Admins only.
 */
public class AdminGetClaudeCodeTranscriptHistoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminGetClaudeCodeTranscriptHistoryReactor.class);

	public AdminGetClaudeCodeTranscriptHistoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			throwFunctionalityOnlyExposedForAdminsError();
		}
		organizeKeys();
		String roomId = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
		if (roomId == null) {
			throw new IllegalArgumentException("Room id is required");
		}

		// the transcript may be on another server; the owner's read gets it by loading
		// the room
		ClusterUtil.pullRoom(roomId);
		List<Object> events = new ArrayList<>();
		Path jsonl = ClaudeCodeTranscriptLocator.findJsonlFile(roomId);
		if (jsonl == null) {
			return new NounMetadata(events, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}

		try (Stream<String> lines = Files.lines(jsonl)) {
			lines.forEach(rawLine -> {
				String line = rawLine == null ? "" : rawLine.trim();
				if (line.isEmpty()) {
					return;
				}
				try {
					JSONObject parsed = ClaudeCodeTranscriptParser.parse(new JSONObject(line));
					if (parsed != null) {
						events.add(parsed.toMap());
					}
				} catch (Exception e) {
					classLogger.warn("Skipping malformed JSONL line in room {}", roomId, e);
				}
			});
		} catch (IOException e) {
			classLogger.error("Unable to read JSONL transcript for room {}", roomId, e);
			throw new IllegalStateException("Unable to read transcript for the requested room id");
		}

		return new NounMetadata(events, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Admins only. Reads any user's room's Claude Code JSONL transcript from disk and returns the parsed events in the same shape the live websocket emits.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "Room identifier whose Claude Code transcript should be read and parsed.";
		}
		return super.getDescriptionForKey(key);
	}
}
