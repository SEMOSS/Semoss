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
package prerna.engine.impl.model.usage;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.access.ModelUsageLimitPath.StepLimitType;

/**
 * The source a call is billed to, as the inference logs record it in
 * {@code BILLED_SOURCE_TYPE}, {@code BILLED_SUBJECT_ID} and
 * {@code BILLED_SUBJECT_TYPE}. Those columns are what a team's or public use's
 * counters are filled from, so a cold counter counts only the calls billed to
 * it.
 *
 * @param sourceType  which source was billed: {@code USER}, {@code TEAM},
 *                    {@code PUBLIC}, {@code ALL_MODELS} or {@code PLATFORM}
 * @param subjectId   the person's primary login id for {@code USER}, the login
 *                    id of the all-models default, or the team id; null
 *                    otherwise
 * @param subjectType the matching login type or team type; null otherwise
 */
public record ModelUsageBilledTo(String sourceType, String subjectId, String subjectType) {

	public static final String USER = "USER";
	public static final String TEAM = "TEAM";
	public static final String PUBLIC = "PUBLIC";
	public static final String ALL_MODELS = "ALL_MODELS";
	public static final String PLATFORM = "PLATFORM";

	/**
	 * The source type the inference logs record, in {@code BILLED_SOURCE_TYPE}, for
	 * the type of limit a call is billed to; null for the Model Total, a ceiling
	 * that is never billed.
	 *
	 * @param limitType the type of limit billed
	 * @return its source type
	 */
	public static String sourceTypeOf(StepLimitType limitType) {
		if (limitType == null) {
			return null;
		}
		switch (limitType) {
		case DIRECT:
			return USER;
		case TEAM:
			return TEAM;
		case PUBLIC:
			return PUBLIC;
		case ALL_MODELS:
			return ALL_MODELS;
		case PLATFORM:
			return PLATFORM;
		default:
			return null;
		}
	}

	/**
	 * The source as a JSON-ready map, for the {@code billedTo} of the usage limits
	 * put on a model's response.
	 *
	 * @return the source as a map of {@code sourceType}, {@code subjectId} and
	 *         {@code subjectType}
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("sourceType", sourceType);
		map.put("subjectId", subjectId);
		map.put("subjectType", subjectType);
		return map;
	}
}
