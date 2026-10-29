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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One of an engine's guardrail hit limits, from the {@code hitLimits} list of
 * its pipeline file: how many hits of the named checks close the room, or lock
 * the user out of the engine. A hit is any check flagging a call, whether it
 * blocks, masks or answers in the engine's place.
 *
 * @param limitId       the limit's id in the pipeline file
 * @param checks        the ids of the checks whose hits count
 * @param hits          how many hits reach the limit
 * @param scope         whose hits count: the room's, or the user's
 * @param windowMinutes for a user limit, how far back hits count; 0 for a room
 *                      limit
 * @param lockMinutes   for a user limit, how long the user is locked out; 0 for
 *                      a room limit
 * @param message       what the refused call returns, or null for the default
 */
public record GuardrailHitLimit(String limitId, Set<String> checks, int hits, Scope scope, int windowMinutes,
		int lockMinutes, String message) {

	public static final String HIT_LIMITS_KEY = "hitLimits";

	private static final String LIMIT_ID_KEY = "limitId";
	private static final String CHECKS_KEY = "checks";
	private static final String HITS_KEY = "hits";
	private static final String SCOPE_KEY = "scope";
	private static final String WINDOW_MINUTES_KEY = "windowMinutes";
	private static final String LOCK_MINUTES_KEY = "lockMinutes";
	private static final String MESSAGE_KEY = "message";
	private static final Set<String> KEYS = Set.of(LIMIT_ID_KEY, CHECKS_KEY, HITS_KEY, SCOPE_KEY, WINDOW_MINUTES_KEY,
			LOCK_MINUTES_KEY, MESSAGE_KEY);
	/** The longest a window or a lock can be: a year */
	private static final int MAX_MINUTES = 366 * 24 * 60;

	/** Whose hits a limit counts */
	public enum Scope {
		/** The room's hits; reaching the limit closes the room */
		ROOM,
		/**
		 * The user's hits in a rolling window; reaching the limit locks the user out of
		 * the engine
		 */
		USER
	}

	/**
	 * Reads and checks a pipeline file's {@code hitLimits} list.
	 *
	 * @param raw      the list, as parsed from JSON or a pixel map, or null for
	 *                 none
	 * @param checkIds the ids of the checks the file defines
	 * @return the limits, in file order
	 * @throws IllegalArgumentException naming the first problem found
	 */
	public static List<GuardrailHitLimit> parseAll(Object raw, Set<String> checkIds) {
		List<GuardrailHitLimit> limits = new ArrayList<>();
		if (raw == null) {
			return limits;
		}
		if (!(raw instanceof List<?> list)) {
			throw new IllegalArgumentException(HIT_LIMITS_KEY + " must be a list");
		}
		Set<String> limitIds = new HashSet<>();
		for (int i = 0; i < list.size(); i++) {
			String path = HIT_LIMITS_KEY + "[" + i + "]";
			if (!(list.get(i) instanceof Map<?, ?> map)) {
				throw new IllegalArgumentException(path + " must be a map");
			}
			GuardrailHitLimit limit = parse(path, map, checkIds);
			if (!limitIds.add(limit.limitId())) {
				throw new IllegalArgumentException(
						path + "." + LIMIT_ID_KEY + " '" + limit.limitId() + "' is used by more than one limit");
			}
			limits.add(limit);
		}
		return limits;
	}

	private static GuardrailHitLimit parse(String path, Map<?, ?> map, Set<String> checkIds) {
		for (Object key : map.keySet()) {
			if (!KEYS.contains(key)) {
				throw new IllegalArgumentException(
						path + " contains unknown key '" + key + "' - allowed keys are " + KEYS);
			}
		}
		Object limitId = map.get(LIMIT_ID_KEY);
		if (!(limitId instanceof String id) || id.isBlank() || id.length() > 255) {
			throw new IllegalArgumentException(path + "." + LIMIT_ID_KEY + " must be a non-empty string");
		}

		if (!(map.get(CHECKS_KEY) instanceof List<?> checkList) || checkList.isEmpty()) {
			throw new IllegalArgumentException(path + "." + CHECKS_KEY + " must list at least one check id");
		}
		Set<String> checks = new LinkedHashSet<>();
		for (Object check : checkList) {
			if (!(check instanceof String checkId) || !checkIds.contains(checkId)) {
				throw new IllegalArgumentException(
						path + "." + CHECKS_KEY + " names '" + check + "', which is not a check in this pipeline file");
			}
			checks.add(checkId);
		}

		int hits = positiveWholeNumber(path + "." + HITS_KEY, map.get(HITS_KEY), Integer.MAX_VALUE);

		Scope scope;
		try {
			scope = Scope.valueOf(String.valueOf(map.get(SCOPE_KEY)));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(path + "." + SCOPE_KEY + " must be ROOM or USER");
		}

		int windowMinutes = 0;
		int lockMinutes = 0;
		if (scope == Scope.USER) {
			windowMinutes = positiveWholeNumber(path + "." + WINDOW_MINUTES_KEY, map.get(WINDOW_MINUTES_KEY),
					MAX_MINUTES);
			lockMinutes = positiveWholeNumber(path + "." + LOCK_MINUTES_KEY, map.get(LOCK_MINUTES_KEY), MAX_MINUTES);
		} else if (map.get(WINDOW_MINUTES_KEY) != null || map.get(LOCK_MINUTES_KEY) != null) {
			throw new IllegalArgumentException(path + ": a ROOM limit counts the room's hits, so it takes no "
					+ WINDOW_MINUTES_KEY + " or " + LOCK_MINUTES_KEY);
		}

		Object message = map.get(MESSAGE_KEY);
		if (message != null && (!(message instanceof String text) || text.isBlank())) {
			throw new IllegalArgumentException(path + "." + MESSAGE_KEY + " must be a non-empty string");
		}
		return new GuardrailHitLimit(id, checks, hits, scope, windowMinutes, lockMinutes, (String) message);
	}

	/**
	 * A whole number of at least 1. A pixel map delivers numbers as doubles, so 3.0
	 * reads as 3.
	 */
	private static int positiveWholeNumber(String path, Object value, int max) {
		if (!(value instanceof Number number) || number.doubleValue() != Math.rint(number.doubleValue())
				|| number.doubleValue() < 1 || number.doubleValue() > max) {
			throw new IllegalArgumentException(path + " must be a whole number from 1 to " + max);
		}
		return number.intValue();
	}

	/**
	 * The limit as its pipeline file stores it, with whole numbers.
	 *
	 * @return the limit's map
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put(LIMIT_ID_KEY, this.limitId);
		map.put(CHECKS_KEY, new ArrayList<>(this.checks));
		map.put(HITS_KEY, this.hits);
		map.put(SCOPE_KEY, this.scope.name());
		if (this.scope == Scope.USER) {
			map.put(WINDOW_MINUTES_KEY, this.windowMinutes);
			map.put(LOCK_MINUTES_KEY, this.lockMinutes);
		}
		if (this.message != null) {
			map.put(MESSAGE_KEY, this.message);
		}
		return map;
	}
}
