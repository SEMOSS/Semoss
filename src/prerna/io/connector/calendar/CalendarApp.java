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
package prerna.io.connector.calendar;

import prerna.auth.AuthProvider;
import prerna.io.connector.IConnectorApp;

/**
 * The calendars the calendar reactors read and write.
 */
public enum CalendarApp implements IConnectorApp {

	MICROSOFT_CALENDAR(AuthProvider.MICROSOFT, "Microsoft 365 calendar", "microsoft", "MicrosoftCalendar"),
	GOOGLE_CALENDAR(AuthProvider.GOOGLE, "Google Calendar", "google", "GoogleCalendar");

	private final AuthProvider authProvider;
	private final String displayName;
	private final String providerId;
	private final String reactorPrefix;

	CalendarApp(AuthProvider authProvider, String displayName, String providerId, String reactorPrefix) {
		this.authProvider = authProvider;
		this.displayName = displayName;
		this.providerId = providerId;
		this.reactorPrefix = reactorPrefix;
	}

	@Override
	public AuthProvider getAuthProvider() {
		return this.authProvider;
	}

	@Override
	public String getDisplayName() {
		return this.displayName;
	}

	@Override
	public String getProviderId() {
		return this.providerId;
	}

	@Override
	public String getReactorPrefix() {
		return this.reactorPrefix;
	}
}
