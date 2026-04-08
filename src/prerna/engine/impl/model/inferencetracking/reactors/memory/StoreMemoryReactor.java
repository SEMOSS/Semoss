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
package prerna.engine.impl.model.inferencetracking.reactors.memory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import prerna.util.Utility;

/**
 * Reactor that persists a user memory to the ModelInferenceLogsDb.
 * <p>
 * Memories are user-scoped facts, preferences, summaries, or episodic notes that
 * the AI can recall in future conversations to provide personalized context.
 * <p>
 * Pixel usage:
 * <pre>
 *   StoreMemory(content=["User prefers bullet-point responses"], memoryType=["PREFERENCE"]);
 *   StoreMemory(content=["Project deadline is April 30"], roomId=["abc-123"]);
 * </pre>
 *
 * @see ModelInferenceLogsUtils#insertMemory(String, String, String, String, String, String)
 */
public class StoreMemoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(StoreMemoryReactor.class);

	/** Request key for the memory category (FACT, PREFERENCE, SUMMARY, EPISODE). Defaults to FACT. */
	private static final String MEMORY_TYPE_KEY = "memoryType";
	/** Request key for optional JSON metadata attached to the memory. */
	private static final String METADATA_KEY = "metadata";

	public StoreMemoryReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.CONTENT.getKey(),
				MEMORY_TYPE_KEY,
				ReactorKeysEnum.ROOM_ID.getKey(),
				METADATA_KEY
		};
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	/**
	 * Stores a new memory record for the authenticated user.
	 * <p>
	 * Required parameters:
	 * <ul>
	 *   <li>{@code content} — the text content of the memory (required)</li>
	 * </ul>
	 * Optional parameters:
	 * <ul>
	 *   <li>{@code memoryType} — category: FACT, PREFERENCE, SUMMARY, or EPISODE (default: FACT)</li>
	 *   <li>{@code roomId} — the source room this memory originated from</li>
	 *   <li>{@code metadata} — arbitrary JSON metadata to attach</li>
	 * </ul>
	 *
	 * @return {@link NounMetadata} map containing {@code memoryId} and {@code status}
	 * @throws IllegalArgumentException if user is not logged in or content is empty
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in to store memories");
		}
		if (user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User authentication token is missing");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String content = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.CONTENT.getKey()));
		if (content == null || content.trim().isEmpty()) {
			throw new IllegalArgumentException("Content is required to store a memory");
		}

		String memoryType = this.keyValue.get(MEMORY_TYPE_KEY);
		if (memoryType == null || memoryType.trim().isEmpty()) {
			memoryType = "FACT";
		} else {
			memoryType = memoryType.toUpperCase().trim();
		}

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String metadata = Utility.decodeURIComponent(this.keyValue.get(METADATA_KEY));

		String memoryId = UUID.randomUUID().toString();

		try {
			ModelInferenceLogsUtils.insertMemory(memoryId, userId, roomId, memoryType, content, metadata);
		} catch (Exception e) {
			classLogger.error("Failed to store memory for user '{}'.", userId, e);
			throw new IllegalArgumentException("Failed to store memory: " + e.getMessage());
		}

		Map<String, Object> output = new HashMap<>();
		output.put("memoryId", memoryId);
		output.put("status", "stored");
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

}
