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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads one event off the calendar of whoever is signed in.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.Read} for {@code GET /me/events/{id}}</li>
 * </ul>
 * 
 * <p>
 * A listing leaves the body out by default, so this is how a caller reads what
 * a meeting is actually about, along with everybody invited and how they
 * replied.
 * </p>
 */
public class MicrosoftCalendarGetEventReactor extends AbstractMicrosoftCalendarReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarGetEventReactor.class);

	private static final String MAX_BODY_CHARS = "maxBodyChars";

	public MicrosoftCalendarGetEventReactor() {
		this.keysToGet = new String[] { EVENT_ID, TIME_ZONE, MAX_BODY_CHARS, CALENDAR_ID };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String eventId = trimToNull(this.keyValue.get(EVENT_ID));
		if (eventId == null) {
			throw new SemossPixelException("An " + EVENT_ID + " is required to read a calendar event.");
		}
		String timeZone = trimToNull(this.keyValue.get(TIME_ZONE));
		String calendarId = trimToNull(this.keyValue.get(CALENDAR_ID));
		int maxBodyChars = positiveInt(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			Map<String, Object> event = MicrosoftCalendarHelper.getEvent(accessToken, calendarId, eventId, maxBodyChars,
					timeZone);
			return new NounMetadata(event, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading calendar event '{}'", eventId, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to read calendar event '{}'", eventId, e);
			throw new SemossPixelException(
					"An error occurred reading the calendar event. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read one event from the signed in user's own Microsoft 365 calendar.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(EVENT_ID)) {
			return "Id of the event to read, as returned by MicrosoftCalendarListEvents.";
		} else if (key.equals(MAX_BODY_CHARS)) {
			return "Optional longest body to return before it is truncated. Defaults to " + DEFAULT_MAX_BODY_CHARS
					+ ".";
		}
		return super.getDescriptionForKey(key);
	}
}
