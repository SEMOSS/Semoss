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

import java.time.Instant;
import java.util.List;

import prerna.auth.User;
import prerna.io.connector.calendar.AbstractGetScheduleReactor;
import prerna.io.connector.calendar.Availability;
import prerna.io.connector.calendar.CalendarApp;
import prerna.io.connector.google.GoogleLoginUtils;

/**
 * Reads when one or more people are busy on their Google calendars.
 *
 * <p>
 * Google says only when somebody is busy, never what with, so every stretch
 * reads as busy.
 * </p>
 *
 * <p>
 * Required Google scope, each under {@code https://www.googleapis.com/auth/}:
 * one of {@code calendar.freebusy}, {@code calendar.readonly} or
 * {@code calendar}.
 * </p>
 */
public class GoogleCalendarGetScheduleReactor extends AbstractGetScheduleReactor {

	@Override
	protected CalendarApp getCalendarApp() {
		return CalendarApp.GOOGLE_CALENDAR;
	}

	@Override
	protected List<Availability> getSchedule(User user, List<String> schedules, Instant start, Instant end)
			throws Exception {
		GoogleCalendarHelper calendar = new GoogleCalendarHelper(GoogleLoginUtils.getValidAccessToken(user));
		return GoogleCalendarEventMapper.toAvailability(calendar.freeBusy(schedules, start, end), schedules);
	}
}
