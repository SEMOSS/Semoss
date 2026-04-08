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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


/**
 * Reactor that lists active memories for the authenticated user with optional filtering and pagination.
 * <p>
 * Pixel usage:
 * <pre>
 *   ListMemories();
 *   ListMemories(memoryType=["PREFERENCE"], limit=[10], offset=[0]);
 * </pre>
 *
 * @see ModelInferenceLogsUtils#getMemoriesForUser(String, String, long, long)
 */
public class ListMemoriesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListMemoriesReactor.class);

	/** Request key for optional memory type filter. */
	private static final String MEMORY_TYPE_KEY = "memoryType";

	public ListMemoriesReactor() {
		this.keysToGet = new String[] {
				MEMORY_TYPE_KEY,
				ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey()
		};
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	/**
	 * Retrieves a paginated list of active memories for the current user.
	 * <p>
	 * Optional parameters:
	 * <ul>
	 *   <li>{@code memoryType} — filter by category (FACT, PREFERENCE, SUMMARY, EPISODE)</li>
	 *   <li>{@code limit} — max results to return (default: 50)</li>
	 *   <li>{@code offset} — pagination offset (default: 0)</li>
	 * </ul>
	 *
	 * @return {@link NounMetadata} map containing {@code memories} list, {@code count}, {@code limit}, {@code offset}
	 * @throws IllegalArgumentException if user is not logged in or pagination values are negative
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in to list memories");
		}
		if (user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User authentication token is missing");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String memoryType = this.keyValue.get(MEMORY_TYPE_KEY);

		long limit = 50;
		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		if (limitStr != null && !limitStr.trim().isEmpty()) {
			limit = Long.parseLong(limitStr);
		}

		long offset = 0;
		String offsetStr = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		if (offsetStr != null && !offsetStr.trim().isEmpty()) {
			offset = Long.parseLong(offsetStr);
		}

		if (limit < 0) {
			throw new IllegalArgumentException("Limit must be non-negative");
		}
		if (offset < 0) {
			throw new IllegalArgumentException("Offset must be non-negative");
		}

		List<Map<String, Object>> memories;
		try {
			memories = ModelInferenceLogsUtils.getMemoriesForUser(userId, memoryType, limit, offset);
		} catch (Exception e) {
			classLogger.error("Failed to list memories for user '{}'.", userId, e);
			throw new IllegalArgumentException("Failed to list memories: " + e.getMessage());
		}

		Map<String, Object> output = new HashMap<>();
		output.put("memories", memories);
		output.put("count", memories.size());
		output.put("limit", limit);
		output.put("offset", offset);
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

}
