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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Takes an event off the calendar of whoever is signed in.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.ReadWrite} for {@code DELETE /me/events/{id}}</li>
 * </ul>
 *
 * <p>
 * What this means depends on whose event it is. An event the signed in user
 * organized is cancelled for everybody invited, and one they were only invited
 * to is removed from their own calendar and leaves everybody else's alone. To
 * turn down an invitation and say so, use
 * {@code MicrosoftCalendarRespondToEvent} instead.
 * </p>
 */
public class MicrosoftCalendarDeleteEventReactor extends AbstractMicrosoftCalendarReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarDeleteEventReactor.class);

	public MicrosoftCalendarDeleteEventReactor() {
		this.keysToGet = new String[] { EVENT_ID, CALENDAR_ID };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String eventId = trimToNull(this.keyValue.get(EVENT_ID));
		if (eventId == null) {
			throw new SemossPixelException("An " + EVENT_ID + " is required to delete a calendar event.");
		}
		String calendarId = trimToNull(this.keyValue.get(CALENDAR_ID));

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			MicrosoftCalendarHelper.deleteEvent(accessToken, calendarId, eventId);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("id", eventId);
			output.put("deleted", true);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting calendar event '{}'", eventId, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete calendar event '{}'", eventId, e);
			throw new SemossPixelException(
					"An error occurred deleting the calendar event. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Delete an event from the signed in user's own Microsoft 365 calendar, cancelling it for the attendees when the user organized it.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(EVENT_ID)) {
			return "Id of the event to delete, as returned by MicrosoftCalendarListEvents.";
		}
		return super.getDescriptionForKey(key);
	}
}
