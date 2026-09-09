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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads what is on the calendar of whoever is signed in.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.Read} for {@code GET /me/calendarView} and
 * {@code GET /me/calendars/{id}/calendarView}</li>
 * </ul>
 *
 * <p>
 * A window rather than a plain list, because only a window expands a recurring
 * meeting into the sittings that actually fall in it. A weekly stand up
 * therefore comes back once for each week asked about, each with its own id
 * that {@code MicrosoftCalendarGetEvent} and the writing reactors accept.
 * </p>
 */
public class MicrosoftCalendarListEventsReactor extends AbstractMicrosoftCalendarReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarListEventsReactor.class);

	private static final String DAYS = "days";
	private static final String SUBJECT = "subject";
	private static final String INCLUDE_BODY = "includeBody";
	private static final String MAX_BODY_CHARS = "maxBodyChars";

	/** How far ahead the window reaches when a caller does not say. */
	private static final int DEFAULT_DAYS = 7;

	/** How many events come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 50;

	/** The most a caller can ask for, so a pixel cannot pull a whole calendar. */
	private static final int MAX_LIMIT = 100;

	public MicrosoftCalendarListEventsReactor() {
		this.keysToGet = new String[] { START, END, DAYS, TIME_ZONE, SUBJECT, ReactorKeysEnum.LIMIT.getKey(),
				INCLUDE_BODY, MAX_BODY_CHARS, CALENDAR_ID };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String timeZone = trimToNull(this.keyValue.get(TIME_ZONE));
		String calendarId = trimToNull(this.keyValue.get(CALENDAR_ID));
		String subject = trimToNull(this.keyValue.get(SUBJECT));
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);
		int days = positiveInt(DAYS, DEFAULT_DAYS, Integer.MAX_VALUE);
		// a listing is usually about when rather than what, and a body each is the
		// bulk of what comes back, so it is left out unless it is asked for
		boolean includeBody = Boolean.parseBoolean(this.keyValue.get(INCLUDE_BODY));
		int maxBodyChars = positiveInt(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);

		// the window starts now and runs a week unless the caller frames it, which
		// is what somebody asking what is on their calendar means
		String start = trimToNull(this.keyValue.get(START));
		if (start == null) {
			start = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
		}
		String end = trimToNull(this.keyValue.get(END));

		try {
			if (end == null) {
				end = MicrosoftCalendarHelper.plusDays(start, days);
			}

			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			List<Map<String, Object>> events = MicrosoftCalendarHelper.listEvents(accessToken, calendarId, start, end,
					subject, includeBody, maxBodyChars, timeZone, limit);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("start", start);
			output.put("end", end);
			output.put("count", events.size());
			output.put("events", events);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading the signed in user's calendar", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read the signed in user's calendar", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read the signed in user's calendar", e);
			throw new SemossPixelException("An error occurred reading your calendar. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read the events on the signed in user's own Microsoft 365 calendar.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(START)) {
			return "Optional start of the window as an ISO 8601 date and time. Defaults to now.";
		} else if (key.equals(END)) {
			return "Optional end of the window as an ISO 8601 date and time. Defaults to the number of days after the start.";
		} else if (key.equals(DAYS)) {
			return "Optional number of days the window covers when no end is given. Defaults to " + DEFAULT_DAYS + ".";
		} else if (key.equals(SUBJECT)) {
			return "Optional text the subject has to contain, matched without regard to case.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of events to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		} else if (key.equals(INCLUDE_BODY)) {
			return "Optional boolean for whether the event body comes back. Defaults to false, since a listing is usually about when rather than what.";
		} else if (key.equals(MAX_BODY_CHARS)) {
			return "Optional longest body to return before it is truncated. Defaults to " + DEFAULT_MAX_BODY_CHARS
					+ ".";
		}
		return super.getDescriptionForKey(key);
	}
}
