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
package prerna.io.connector.google.calendar;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ConnectorPage;
import prerna.io.connector.calendar.AbstractListCalendarsReactor;
import prerna.io.connector.calendar.CalendarApp;
import prerna.io.connector.calendar.CalendarInfo;
import prerna.io.connector.google.GoogleLoginUtils;

/**
 * Lists the Google calendars of the signed in user.
 *
 * <p>
 * Google lists only the user's own calendar list, so somebody else's calendars
 * are their primary one, read by their address, unless the user added more of
 * theirs to the list.
 * </p>
 *
 * <p>
 * Required Google scope, each under {@code https://www.googleapis.com/auth/}:
 * one of {@code calendar.readonly} or {@code calendar}.
 * </p>
 */
public class GoogleCalendarListCalendarsReactor extends AbstractListCalendarsReactor {

	@Override
	protected CalendarApp getCalendarApp() {
		return CalendarApp.GOOGLE_CALENDAR;
	}

	@Override
	protected ConnectorPage<CalendarInfo> listCalendars(User user, String mailbox, int offset, int limit)
			throws Exception {
		GoogleCalendarHelper calendar = new GoogleCalendarHelper(GoogleLoginUtils.getValidAccessToken(user));
		String account = accountEmail(user);
		List<Map<String, Object>> entries = calendar.listCalendars();
		if (mailbox == null) {
			return ConnectorPage.of(
					entries.stream().map(entry -> GoogleCalendarEventMapper.toCalendar(entry, account)).toList(),
					offset, limit);
		}
		for (Map<String, Object> entry : entries) {
			if (mailbox.equalsIgnoreCase(String.valueOf(entry.get("id")))) {
				return ConnectorPage.of(List.of(GoogleCalendarEventMapper.toCalendar(entry, account)), offset, limit);
			}
		}
		return ConnectorPage.of(List.of(GoogleCalendarEventMapper.toSharedCalendar(calendar.getCalendar(mailbox))),
				offset, limit);
	}
}
