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

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the calendars of whoever is signed in.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.Read} for {@code GET /me/calendars}</li>
 * </ul>
 * 
 * <p>
 * The id of a calendar here is what the other calendar reactors take as their
 * {@code calendarId}, and the one marked {@code isDefaultCalendar} is the one
 * they use when that key is left out.
 * </p>
 */
public class MicrosoftCalendarListCalendarsReactor extends AbstractMicrosoftCalendarReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarListCalendarsReactor.class);

	public MicrosoftCalendarListCalendarsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), 0, Integer.MAX_VALUE);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			List<Map<String, Object>> calendars = MicrosoftCalendarHelper.listCalendars(accessToken, limit);
			return new NounMetadata(calendars, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing the signed in user's Microsoft calendars", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list the signed in user's Microsoft calendars", e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of calendars. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "List the Microsoft 365 calendars of the signed in user.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of calendars to return. All calendars are returned when omitted.";
		}
		return super.getDescriptionForKey(key);
	}
}
