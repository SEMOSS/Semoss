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
 * Changes an event on the calendar of whoever is signed in.
 *
 * <p>
 * Only the fields passed are changed, so moving a meeting an hour later means
 * passing the event id and the new times and nothing else. Whoever is invited
 * is told about a change to the time, the place or the attendees the way
 * Outlook tells them.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.ReadWrite} for {@code PATCH /me/events/{id}}</li>
 * </ul>
 *
 * <p>
 * Passing attendees replaces the guest list rather than adding to it, which is
 * how Graph reads the field, so a caller adding somebody passes everybody.
 * </p>
 */
public class MicrosoftCalendarUpdateEventReactor extends AbstractMicrosoftCalendarEventReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarUpdateEventReactor.class);

	public MicrosoftCalendarUpdateEventReactor() {
		// the event to change, then everything that can be changed about it
		this.keysToGet = new String[EVENT_KEYS.length + 1];
		this.keysToGet[0] = EVENT_ID;
		System.arraycopy(EVENT_KEYS, 0, this.keysToGet, 1, EVENT_KEYS.length);
		this.keyRequired = new int[this.keysToGet.length];
		this.keyRequired[0] = 1;
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String eventId = trimToNull(this.keyValue.get(EVENT_ID));
		if (eventId == null) {
			throw new SemossPixelException("An " + EVENT_ID + " is required to change a calendar event.");
		}
		Map<String, Object> changes = composeEvent(false, "change on the calendar event");
		String calendarId = trimToNull(this.keyValue.get(CALENDAR_ID));

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			Map<String, Object> updated = MicrosoftCalendarHelper.updateEvent(accessToken, calendarId, eventId, changes,
					DEFAULT_MAX_BODY_CHARS, requestedTimeZone());
			return new NounMetadata(updated, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while changing calendar event '{}'", eventId, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to change calendar event '{}'", eventId, e);
			throw new SemossPixelException(
					"An error occurred changing the calendar event. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Change an event on the signed in user's own Microsoft 365 calendar, leaving whatever is not passed as it was.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(EVENT_ID)) {
			return "Id of the event to change, as returned by MicrosoftCalendarListEvents.";
		} else if (key.equals(ATTENDEES)) {
			return "Optional email addresses of the required attendees. Passing any attendees replaces the whole guest list, so pass everybody who should be on it.";
		} else if (key.equals(OPTIONAL_ATTENDEES)) {
			return "Optional email addresses of the optional attendees. Passing any attendees replaces the whole guest list, so pass everybody who should be on it.";
		}
		return super.getDescriptionForKey(key);
	}
}
