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
package prerna.io.connector.ms.calendar;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * What every calendar reactor has in common.
 *
 * <p>
 * The keys naming a calendar, an event and a time zone mean the same thing
 * wherever they appear, so they are named and described once here rather than
 * once per reactor. The readers of those keys are here for the same reason:
 * every one of these reactors has to turn pixel strings into numbers, booleans
 * and lists, and the way a bad value is reported should not depend on which
 * reactor happened to read it.
 * </p>
 */
public abstract class AbstractMicrosoftCalendarReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractMicrosoftCalendarReactor.class);

	protected static final String CALENDAR_ID = "calendarId";
	protected static final String EVENT_ID = "eventId";
	protected static final String TIME_ZONE = "timeZone";
	protected static final String START = "start";
	protected static final String END = "end";

	/** How much of a body comes back before it is cut short. */
	protected static final int DEFAULT_MAX_BODY_CHARS = 10_000;

	/**
	 * Read a key that has to be a positive whole number.
	 *
	 * @param key      the key to read
	 * @param fallback what it is when the caller left it out
	 * @param cap      the largest value allowed, which the value is held down to
	 * @return the value
	 */
	protected int positiveInt(String key, int fallback, int cap) {
		Integer parsed = optionalInt(key);
		if (parsed == null) {
			return fallback;
		}
		if (parsed <= 0) {
			throw new SemossPixelException(key + " must be greater than 0.");
		}
		if (parsed > cap) {
			classLogger.warn("A {} of {} was asked for, using {} instead", key, parsed, cap);
			return cap;
		}
		return parsed;
	}

	/**
	 * Read a key that has to be a whole number when it is there at all.
	 *
	 * @param key the key to read
	 * @return the value, or null when the caller left it out
	 */
	protected Integer optionalInt(String key) {
		String value = trimToNull(this.keyValue.get(key));
		if (value == null) {
			return null;
		}
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			classLogger.error("Invalid {} of '{}' passed to a Microsoft calendar reactor", key, value, e);
			throw new SemossPixelException(key + " must be a whole number.");
		}
	}

	/**
	 * Read a key that has to be true or false when it is there at all.
	 *
	 * @param key the key to read
	 * @return the value, or null when the caller left it out, which is what lets a
	 *         change leave the field as it was rather than setting it to false
	 */
	protected Boolean optionalBoolean(String key) {
		String value = trimToNull(this.keyValue.get(key));
		if (value == null) {
			return null;
		}
		return Boolean.valueOf(Boolean.parseBoolean(value));
	}

	/**
	 * Read one set of values, which a caller may pass as several nouns, as one
	 * comma separated noun, or as any mixture of the two.
	 *
	 * @param key the key to read
	 * @return the values, or null when none were passed
	 */
	protected String[] values(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<String> values = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			Object value = grs.getNoun(i).getValue();
			if (value == null) {
				continue;
			}
			// a single value can still be a comma separated list, since that is how
			// somebody writing the pixel by hand tends to pass more than one
			for (String entry : value.toString().split(",")) {
				if (!entry.trim().isEmpty()) {
					values.add(entry.trim());
				}
			}
		}
		if (values.isEmpty()) {
			return null;
		}
		return values.toArray(new String[0]);
	}

	/**
	 * @param value the value to trim
	 * @return the value without surrounding space, or null when there is nothing
	 *         left of it
	 */
	protected static String trimToNull(String value) {
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		return value.trim();
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CALENDAR_ID)) {
			return "Optional id of the calendar to work against. The signed in user's default calendar is used when omitted.";
		} else if (key.equals(EVENT_ID)) {
			return "Id of the calendar event.";
		} else if (key.equals(TIME_ZONE)) {
			return "Optional time zone that times without an offset are read and answered in, such as Eastern Standard Time or America/New_York. Defaults to UTC.";
		} else if (key.equals(START)) {
			return "Start as an ISO 8601 date and time, such as 2026-09-01T13:00:00 in the given time zone or 2026-09-01T17:00:00Z in UTC.";
		} else if (key.equals(END)) {
			return "End as an ISO 8601 date and time, such as 2026-09-01T14:00:00 in the given time zone or 2026-09-01T18:00:00Z in UTC.";
		}
		return super.getDescriptionForKey(key);
	}

}
