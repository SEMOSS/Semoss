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
 * Puts an event on the calendar of whoever is signed in.
 *
 * <p>
 * The event is organized by the signed in user because the token says who that
 * is, so this cannot be used to book time on somebody else's calendar. Naming
 * attendees invites them, which sends each of them an invitation from the
 * organizer in the ordinary way.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.ReadWrite} for {@code POST /me/events}</li>
 * <li>{@code OnlineMeetings.ReadWrite} as well, when {@code isOnlineMeeting}
 * asks for a Teams link</li>
 * </ul>
 *
 * <p>
 * For finding a time everybody can make before booking it, read the free and
 * busy view with {@code MicrosoftCalendarGetSchedule} first.
 * </p>
 */
public class MicrosoftCalendarCreateEventReactor extends AbstractMicrosoftCalendarEventReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarCreateEventReactor.class);

	public MicrosoftCalendarCreateEventReactor() {
		this.keysToGet = EVENT_KEYS.clone();
		this.keyRequired = new int[] { 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		Map<String, Object> event = composeEvent(true, "create a calendar event");
		String calendarId = trimToNull(this.keyValue.get(CALENDAR_ID));

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			Map<String, Object> created = MicrosoftCalendarHelper.createEvent(accessToken, calendarId, event,
					DEFAULT_MAX_BODY_CHARS, requestedTimeZone());
			return new NounMetadata(created, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while creating an event on the signed in user's calendar", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create an event on the signed in user's calendar", e);
			throw new SemossPixelException(
					"An error occurred creating the calendar event. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Create an event on the signed in user's own Microsoft 365 calendar, inviting anybody named as an attendee.";
	}
}
