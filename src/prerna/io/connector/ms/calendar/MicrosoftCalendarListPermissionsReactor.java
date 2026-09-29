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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads who a calendar is shared with and what each of them may do with it.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.Read} for {@code GET /me/calendar/calendarPermissions}
 * and {@code GET /me/calendars/{id}/calendarPermissions}</li>
 * <li>{@code Calendars.Read.Shared} as well, when a {@code mailbox} names
 * somebody else, for {@code GET /users/{id}/calendar/calendarPermissions}</li>
 * </ul>
 *
 * <p>
 * This is what tells a share from a delegation. Both are a permission on a
 * calendar, and the {@code role} is the difference: a reader or a writer has
 * been shared the calendar, while a delegate has also been given the right to
 * receive and answer meeting requests on the owner's behalf, which is what
 * {@code MicrosoftCalendarRespondToEvent} needs a mailbox for.
 * </p>
 *
 * <p>
 * Read against the signed in user's own calendar, this answers who they have
 * shared it with. Read against somebody else's, by naming their mailbox, it
 * answers what that person granted the signed in user.
 * </p>
 */
public class MicrosoftCalendarListPermissionsReactor extends AbstractMicrosoftCalendarReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarListPermissionsReactor.class);

	public MicrosoftCalendarListPermissionsReactor() {
		this.keysToGet = new String[] { CALENDAR_ID, MAILBOX };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String calendarId = trimToNull(this.keyValue.get(CALENDAR_ID));
		String mailbox = trimToNull(this.keyValue.get(MAILBOX));

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			List<Map<String, Object>> permissions = MicrosoftCalendarHelper.listCalendarPermissions(accessToken,
					mailbox, calendarId);

			Map<String, Object> output = new LinkedHashMap<>();
			if (mailbox != null) {
				output.put(MAILBOX, mailbox);
			}
			if (calendarId != null) {
				output.put(CALENDAR_ID, calendarId);
			}
			output.put("count", permissions.size());
			output.put("permissions", permissions);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading the permissions on a Microsoft calendar", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read the permissions on a Microsoft calendar", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read the permissions on a Microsoft calendar", e);
			throw new SemossPixelException(
					"An error occurred retrieving the calendar permissions. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read who a Microsoft 365 calendar is shared with, and whether each of them is a reader, a writer or a delegate.";
	}
}
