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

import java.util.Objects;

import prerna.engine.impl.model.inferencetracking.ModelUsageLogFilter;

/**
 * Whose use a usage limit counter counts. With a rule's metric and a window
 * bucket it makes one counter.
 *
 * <ul>
 * <li>{@link ScopeType#PERSON_MODEL}: all of a person's use of one model,
 * whichever source paid; for direct limits and public use rules for each
 * person.</li>
 * <li>{@link ScopeType#TEAM_SHARED} and {@link ScopeType#TEAM_MEMBER}: the
 * calls billed to a team on one model, together or for one member.</li>
 * <li>{@link ScopeType#PUBLIC_SHARED}: the calls billed to a model's public
 * use.</li>
 * <li>{@link ScopeType#TEAM_TOTAL}: the calls billed to a team on any model; a
 * router's own calls are left out so a routed call counts once.</li>
 * <li>{@link ScopeType#MODEL_TOTAL}: every call a model answers.</li>
 * <li>{@link ScopeType#PERSON_ALL_MODELS}: a person's use of every model, for
 * the all-models and platform defaults; a router's own calls are left out so a
 * routed call counts once.</li>
 * </ul>
 *
 * @param scopeType whose use it counts
 * @param engineId  the model, or null across all models
 * @param teamId    the team, or null
 * @param teamType  the team's type, or null
 * @param userId    the person's primary login id, or null
 * @param userType  the person's primary login type, or null
 */
public record ModelUsageCounterScope(ScopeType scopeType, String engineId, String teamId, String teamType,
		String userId, String userType) {

	/** Whose use a counter counts */
	public enum ScopeType {
		PERSON_MODEL, TEAM_SHARED, TEAM_MEMBER, PUBLIC_SHARED, TEAM_TOTAL, MODEL_TOTAL, PERSON_ALL_MODELS
	}

	public static ModelUsageCounterScope personModel(String engineId, String userId, String userType) {
		return new ModelUsageCounterScope(ScopeType.PERSON_MODEL, engineId, null, null, userId, userType);
	}

	public static ModelUsageCounterScope teamShared(String engineId, String teamId, String teamType) {
		return new ModelUsageCounterScope(ScopeType.TEAM_SHARED, engineId, teamId, teamType, null, null);
	}

	public static ModelUsageCounterScope teamMember(String engineId, String teamId, String teamType, String userId,
			String userType) {
		return new ModelUsageCounterScope(ScopeType.TEAM_MEMBER, engineId, teamId, teamType, userId, userType);
	}

	public static ModelUsageCounterScope publicShared(String engineId) {
		return new ModelUsageCounterScope(ScopeType.PUBLIC_SHARED, engineId, null, null, null, null);
	}

	public static ModelUsageCounterScope teamTotal(String teamId, String teamType) {
		return new ModelUsageCounterScope(ScopeType.TEAM_TOTAL, null, teamId, teamType, null, null);
	}

	public static ModelUsageCounterScope modelTotal(String engineId) {
		return new ModelUsageCounterScope(ScopeType.MODEL_TOTAL, engineId, null, null, null, null);
	}

	public static ModelUsageCounterScope personAllModels(String userId, String userType) {
		return new ModelUsageCounterScope(ScopeType.PERSON_ALL_MODELS, null, null, null, userId, userType);
	}

	/**
	 * The scope's name within a counter key: its type, then the model, team and
	 * person it covers.
	 *
	 * @return the scope's part of a counter key; each part is escaped, so an id
	 *         that holds {@code |} cannot make two scopes share a key
	 */
	public String id() {
		return scopeType.name() + "|" + part(engineId) + "|" + part(teamType) + "|" + part(teamId) + "|"
				+ part(userType) + "|" + part(userId);
	}

	/**
	 * The rows match what the counter counts: billed rows for a team or public use,
	 * every row of the person or model otherwise, and the scopes across all models
	 * leave out a router's own rows, as their counters do.
	 *
	 * @return the {@code MESSAGE} rows the counter is filled from
	 */
	public ModelUsageLogFilter logFilter() {
		switch (scopeType) {
		case PERSON_MODEL:
			return new ModelUsageLogFilter(engineId, userId, userType, null, null, null, false);
		case TEAM_SHARED:
			return new ModelUsageLogFilter(engineId, null, null, ModelUsageBilledTo.TEAM, teamId, teamType, false);
		case TEAM_MEMBER:
			return new ModelUsageLogFilter(engineId, userId, userType, ModelUsageBilledTo.TEAM, teamId, teamType,
					false);
		case PUBLIC_SHARED:
			return new ModelUsageLogFilter(engineId, null, null, ModelUsageBilledTo.PUBLIC, null, null, false);
		case TEAM_TOTAL:
			return new ModelUsageLogFilter(null, null, null, ModelUsageBilledTo.TEAM, teamId, teamType, true);
		case MODEL_TOTAL:
			return new ModelUsageLogFilter(engineId, null, null, null, null, null, false);
		case PERSON_ALL_MODELS:
			return new ModelUsageLogFilter(null, userId, userType, null, null, null, true);
		default:
			throw new IllegalStateException("Unknown counter scope " + scopeType);
		}
	}

	private static String part(String value) {
		return Objects.toString(value, "").replace("%", "%25").replace("|", "%7C");
	}
}
