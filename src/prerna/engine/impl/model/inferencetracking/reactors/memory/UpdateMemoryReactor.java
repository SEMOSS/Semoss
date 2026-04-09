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
 * Reactor that updates the content of an existing user memory.
 * <p>
 * Only updates the CONTENT and DATE_UPDATED fields — the memory type, metadata,
 * and provenance (room_id) remain unchanged. Ownership is enforced: the user
 * must own the memory to update it.
 * <p>
 * Pixel usage:
 * <pre>
 *   UpdateMemory(id=["memory-uuid"], content=["Updated content text"]);
 * </pre>
 *
 * @see ModelInferenceLogsUtils#updateMemoryContent(String, String, String)
 */
public class UpdateMemoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateMemoryReactor.class);

	public UpdateMemoryReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ID.getKey(),
				ReactorKeysEnum.CONTENT.getKey()
		};
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in to update memories");
		}
		if (user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User authentication token is missing");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String memoryId = this.keyValue.get(ReactorKeysEnum.ID.getKey());
		if (memoryId == null || memoryId.trim().isEmpty()) {
			throw new IllegalArgumentException("Memory ID is required");
		}

		String content = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.CONTENT.getKey()));
		if (content == null || content.trim().isEmpty()) {
			throw new IllegalArgumentException("Content is required to update a memory");
		}

		try {
			ModelInferenceLogsUtils.updateMemoryContent(memoryId, userId, content);
		} catch (Exception e) {
			classLogger.error("Failed to update memory '{}' for user '{}'.", memoryId, userId, e);
			throw new IllegalArgumentException("Failed to update memory: " + e.getMessage());
		}

		Map<String, Object> output = new HashMap<>();
		output.put("memoryId", memoryId);
		output.put("status", "updated");
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

}
