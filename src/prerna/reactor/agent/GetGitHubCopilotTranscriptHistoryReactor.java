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
package prerna.reactor.agent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;



/**
 * One-shot reactor that reads an existing room's GitHub Copilot JSONL
 * transcript from disk and returns normalized events in the same shape the live
 * streaming path emits.
 */
public class GetGitHubCopilotTranscriptHistoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetGitHubCopilotTranscriptHistoryReactor.class);

	public GetGitHubCopilotTranscriptHistoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("Room id is required");
		}
		
		List<Object> events = new ArrayList<>();

		String finalRoomId = roomId.trim();
		try {
			RoomUtils.getOrLoadRoom(finalRoomId, this.insight);
		} catch (Exception e) {
			return new NounMetadata(events, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}

		
		Set<String> suppressedToolCallIds = new HashSet<>();
		Path jsonl = GitHubCopilotTranscriptLocator.findJsonlFile(finalRoomId);
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
					List<JSONObject> parsedEvents = GitHubCopilotTranscriptParser.parse(new JSONObject(line), finalRoomId,
							suppressedToolCallIds);
					for (JSONObject event : parsedEvents) {
						if (event != null) {
							events.add(event.toMap());
						}
					}
				} catch (Exception e) {
					classLogger.warn("Skipping malformed GitHub Copilot JSONL line in room {}", finalRoomId, e);
				}
			});
		} catch (IOException e) {
			classLogger.error("Unable to read GitHub Copilot JSONL transcript for room {}", finalRoomId, e);
			throw new IllegalStateException("Unable to read transcript for the requested room id");
		}

		return new NounMetadata(events, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Reads an existing room's GitHub Copilot JSONL transcript from disk and returns parsed events in the same shape the live stream emits.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "Room identifier whose GitHub Copilot transcript should be read and parsed.";
		}
		return super.getDescriptionForKey(key);
	}
}
