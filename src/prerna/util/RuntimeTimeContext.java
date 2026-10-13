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
package prerna.util;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** A server-clock snapshot whose UTC and local values describe the same instant. */
public record RuntimeTimeContext(Instant now, ZoneId timeZone) {

	public RuntimeTimeContext {
		Objects.requireNonNull(now, "now");
		Objects.requireNonNull(timeZone, "timeZone");
	}

	public static RuntimeTimeContext capture(ZoneId timeZone) {
		return new RuntimeTimeContext(Instant.now(), timeZone == null ? ZoneId.of("UTC") : timeZone);
	}

	/** Invalid or missing profile zones fall back to the authenticated user's zone, then UTC. */
	public static ZoneId resolveZone(String profileZone, ZoneId userZone) {
		if (profileZone != null && !profileZone.isBlank()) {
			try {
				return ZoneId.of(profileZone.trim());
			} catch (DateTimeException e) {
				// A stored label may not be a valid Java timezone identifier.
			}
		}
		return userZone == null ? ZoneId.of("UTC") : userZone;
	}

	public Map<String, Object> toMap() {
		ZonedDateTime local = now.atZone(timeZone);
		Map<String, Object> values = new LinkedHashMap<>();
		values.put("nowUtc", now.toString());
		values.put("localDateTime", local.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		values.put("today", local.toLocalDate().toString());
		values.put("weekday", local.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH));
		values.put("timeZone", timeZone.getId());
		return values;
	}

	/** Dynamic data belongs at the conversation tail, never in the cached system prefix. */
	public String guidance() {
		Map<String, Object> values = toMap();
		return "Current time from the SEMOSS server clock: UTC=" + values.get("nowUtc")
				+ "; local=" + values.get("localDateTime") + "; today=" + values.get("today")
				+ " (" + values.get("weekday") + "); timezone=" + values.get("timeZone")
				+ ". Use this latest clock rather than earlier runtime clocks.";
	}
}
