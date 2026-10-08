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

import java.util.Locale;

import prerna.auth.User;
import prerna.io.connector.ConnectorPage;
import prerna.io.connector.calendar.AbstractListEventsReactor;
import prerna.io.connector.calendar.CalendarApp;
import prerna.io.connector.calendar.CalendarEvent;
import prerna.io.connector.google.GoogleLoginUtils;

/**
 * Reads the events on a Google calendar, the signed in user's own or one shared
 * with them.
 *
 * <p>
 * Required Google scope, each under {@code https://www.googleapis.com/auth/}:
 * one of {@code calendar.events.readonly}, {@code calendar.readonly},
 * {@code calendar.events} or {@code calendar}.
 * </p>
 */
public class GoogleCalendarListEventsReactor extends AbstractListEventsReactor {

	@Override
	protected CalendarApp getCalendarApp() {
		return CalendarApp.GOOGLE_CALENDAR;
	}

	@Override
	protected ConnectorPage<CalendarEvent> listEvents(User user, ListEventsRequest request) throws Exception {
		GoogleCalendarHelper calendar = new GoogleCalendarHelper(GoogleLoginUtils.getValidAccessToken(user));
		String wanted = request.subject() == null ? null : request.subject().toLowerCase(Locale.ROOT);
		// google searches every field for text, so a subject is matched here to mean
		// what it means for every calendar
		GoogleCalendarHelper.EventRun run = calendar.listEvents(
				GoogleCalendarHelper.calendarOf(request.calendarId(), request.mailbox()), request.start(),
				request.end(),
				event -> wanted == null || (event.get("summary") != null
						&& event.get("summary").toString().toLowerCase(Locale.ROOT).contains(wanted)),
				request.needed());
		ConnectorPage<CalendarEvent> page = ConnectorPage.of(
				run.events().stream().map(event -> GoogleCalendarEventMapper.toEvent(event, run.timeZone())).toList(),
				request.offset(), request.limit());
		return new ConnectorPage<>(page.items(), page.hasMore() || run.hasMore());
	}
}
