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
package prerna.engine.impl.pipeline;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.access.EngineGrantee;
import prerna.auth.access.UserAccess;

/**
 * What the access check decided about a call to an engine or a project: why it
 * let the call through, with the details behind that, and the app, agent or
 * engine the call counts as made by, which the engine called reads for its
 * usage limits ({@link EngineCallPermission}).
 *
 * @param reason  why the call went through, or null for a call the check does
 *                not look at, such as a method of {@code Object}
 * @param grantee the app, agent or engine making the call, for the engine's
 *                usage limits; null when it is the person's or the platform's
 * @param details the specifics behind the reason, such as the level, the team,
 *                or whose permission was used
 */
public record EngineAccessDecision(EngineAccessReason reason, EngineGrantee grantee, Map<String, Object> details) {

	/** The way an app, an agent or an engine came to make a call */
	public static final String VIA_APP_CODE = "APP_CODE";
	/** The way an agent's run made a call */
	public static final String VIA_AGENT_RUN = "AGENT_RUN";
	/** The way an engine's own code made a call */
	public static final String VIA_ENGINE_CODE = "ENGINE_CODE";
	/** The way the platform made a call for the person */
	public static final String VIA_PLATFORM = "PLATFORM";

	/** A call the check does not look at */
	static final EngineAccessDecision UNCHECKED = new EngineAccessDecision(null, null, Map.of());

	private static final Gson GSON = new Gson();

	/**
	 * Keeps the details as given, leaving out the keys whose value is null.
	 */
	public EngineAccessDecision {
		Map<String, Object> kept = new LinkedHashMap<>();
		if (details != null) {
			details.forEach((key, value) -> {
				if (value != null) {
					kept.put(key, value);
				}
			});
		}
		details = Collections.unmodifiableMap(kept);
	}

	/**
	 * A call the person's own access covers: their grant, a team's, or the engine
	 * or project being public.
	 *
	 * @param access  the person's access
	 * @param grantee the app, agent or engine making the call, for the engine's
	 *                usage limits, or null
	 * @return the decision
	 */
	static EngineAccessDecision person(UserAccess access, EngineGrantee grantee) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("level", access.level().getPermission());
		details.put("teamId", access.teamId());
		details.put("teamType", access.teamType());
		EngineAccessReason reason = switch (access.source()) {
		case TEAM -> EngineAccessReason.TEAM;
		case PUBLIC -> EngineAccessReason.PUBLIC;
		default -> EngineAccessReason.DIRECT;
		};
		return new EngineAccessDecision(reason, grantee, details);
	}

	/**
	 * A call a permission the engine or project gave an app, an agent or another
	 * engine covers.
	 *
	 * @param level         the permission's level
	 * @param given         the app, agent or engine given it
	 * @param via           how that one made the call: {@link #VIA_APP_CODE},
	 *                      {@link #VIA_AGENT_RUN} or {@link #VIA_ENGINE_CODE}
	 * @param callingEngine the engine whose own code made the call, or null
	 * @param grantee       the app, agent or engine making the call, for the
	 *                      engine's usage limits, or null on a project
	 * @return the decision
	 */
	static EngineAccessDecision delegated(AccessPermissionEnum level, EngineGrantee given, String via,
			EngineGrantee callingEngine, EngineGrantee grantee) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("level", level.getPermission());
		details.put("granteeId", given.engineId());
		details.put("granteeType", given.engineType());
		details.put("via", via);
		details.put("callingEngineId", callingEngine == null ? null : callingEngine.engineId());
		return new EngineAccessDecision(EngineAccessReason.DELEGATED, grantee, details);
	}

	/**
	 * A call let through for a reason that needs no level, with one detail.
	 *
	 * @param reason the reason
	 * @param key    the detail's name, or null for none
	 * @param value  the detail's value, or null for none
	 * @return the decision
	 */
	static EngineAccessDecision of(EngineAccessReason reason, String key, Object value) {
		Map<String, Object> details = new LinkedHashMap<>();
		if (key != null) {
			details.put(key, value);
		}
		return new EngineAccessDecision(reason, null, details);
	}

	/**
	 * The details as JSON, for the Activity Log.
	 *
	 * @return the JSON, or null when there are none
	 */
	public String detailsJson() {
		return this.details.isEmpty() ? null : GSON.toJson(this.details);
	}

}
