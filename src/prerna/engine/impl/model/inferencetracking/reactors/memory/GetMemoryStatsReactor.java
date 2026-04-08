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
 * Reactor that returns aggregate memory statistics for the authenticated user.
 * <p>
 * Returns a total count of active memories and a breakdown by memory type
 * (FACT, PREFERENCE, SUMMARY, EPISODE).
 * <p>
 * Pixel usage:
 * <pre>
 *   GetMemoryStats();
 * </pre>
 *
 * @see ModelInferenceLogsUtils#getMemoryStats(String)
 */
public class GetMemoryStatsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetMemoryStatsReactor.class);

	public GetMemoryStatsReactor() {
		this.keysToGet = new String[] {};
		this.keyRequired = new int[] {};
	}

	/**
	 * Retrieves memory statistics for the current user.
	 * <p>
	 * Takes no parameters. Returns a map with:
	 * <ul>
	 *   <li>{@code total} — total number of active memories</li>
	 *   <li>{@code byType} — map of memory type to count</li>
	 * </ul>
	 *
	 * @return {@link NounMetadata} map containing {@code total} and {@code byType}
	 * @throws IllegalArgumentException if user is not logged in
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in to view memory stats");
		}
		if (user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User authentication token is missing");
		}
		String userId = user.getPrimaryLoginToken().getId();

		Map<String, Object> stats;
		try {
			stats = ModelInferenceLogsUtils.getMemoryStats(userId);
		} catch (Exception e) {
			classLogger.error("Failed to get memory stats for user '{}'.", userId, e);
			throw new IllegalArgumentException("Failed to get memory stats: " + e.getMessage());
		}
		return new NounMetadata(stats, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

}
