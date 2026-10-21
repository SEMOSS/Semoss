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

import prerna.auth.User;
import prerna.io.connector.calendar.AbstractUpdateEventReactor;
import prerna.io.connector.calendar.CalendarApp;
import prerna.io.connector.calendar.CalendarEvent;
import prerna.io.connector.calendar.EventRequest;
import prerna.io.connector.ms.MicrosoftLoginUtils;

/**
 * Changes an event on a Microsoft 365 calendar, the signed in user's own or one
 * shared with them to write, leaving whatever is not passed as it was.
 *
 * <p>
 * Required delegated Microsoft Graph scope: {@code Calendars.ReadWrite}, and
 * {@code Calendars.ReadWrite.Shared} to write somebody else's.
 * </p>
 */
public class MicrosoftCalendarUpdateEventReactor extends AbstractUpdateEventReactor {

	public MicrosoftCalendarUpdateEventReactor() {
		// only Outlook keeps how urgent an event is
		super(IMPORTANCE);
	}

	@Override
	protected CalendarApp getCalendarApp() {
		return CalendarApp.MICROSOFT_CALENDAR;
	}

	@Override
	protected void checkProviderValues(EventRequest request) {
		MicrosoftCalendarHelper.checkEventValues(request);
	}

	@Override
	protected String describeProviderValues(String key) {
		return MicrosoftCalendarHelper.describeEventValues(key);
	}

	@Override
	protected CalendarEvent updateEvent(User user, EventRequest request) throws Exception {
		return MicrosoftCalendarHelper.updateEvent(MicrosoftLoginUtils.getValidAccessToken(user), request);
	}
}
