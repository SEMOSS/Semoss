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
import prerna.sablecc2.om.nounmeta.NounMetadata;


/**
 * Reactor that soft-deletes a memory by setting its {@code IS_ACTIVE} flag to false.
 * <p>
 * Only the owning user can delete their own memories. The memory is not physically
 * removed from the database, allowing for potential recovery or audit.
 * <p>
 * Pixel usage:
 * <pre>
 *   DeleteMemory(memoryId=["abc-123-def"]);
 * </pre>
 *
 * @see ModelInferenceLogsUtils#deleteMemory(String, String)
 */
public class DeleteMemoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteMemoryReactor.class);

	/** Request key for the memory to delete. */
	private static final String MEMORY_ID_KEY = "memoryId";

	public DeleteMemoryReactor() {
		this.keysToGet = new String[] { MEMORY_ID_KEY };
		this.keyRequired = new int[] { 1 };
	}

	/**
	 * Soft-deletes the specified memory for the authenticated user.
	 * <p>
	 * Required parameters:
	 * <ul>
	 *   <li>{@code memoryId} — the unique identifier of the memory to delete</li>
	 * </ul>
	 *
	 * @return {@link NounMetadata} map containing {@code memoryId} and {@code status}
	 * @throws IllegalArgumentException if user is not logged in, memoryId is missing,
	 *         or the memory does not belong to the user
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in to delete memories");
		}
		if (user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User authentication token is missing");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String memoryId = this.keyValue.get(MEMORY_ID_KEY);
		if (memoryId == null || memoryId.trim().isEmpty()) {
			throw new IllegalArgumentException("Memory ID is required");
		}

		try {
			ModelInferenceLogsUtils.deleteMemory(memoryId, userId);
		} catch (Exception e) {
			classLogger.error("Failed to delete memory '{}' for user '{}'.", memoryId, userId, e);
			throw new IllegalArgumentException("Failed to delete memory: " + e.getMessage());
		}

		Map<String, Object> output = new HashMap<>();
		output.put("memoryId", memoryId);
		output.put("status", "deleted");
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

}
