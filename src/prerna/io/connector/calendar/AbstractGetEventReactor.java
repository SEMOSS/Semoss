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

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads one event in full.
 */
public abstract class AbstractGetEventReactor extends AbstractCalendarReactor {

	private static final String[] KEYS = { ID, MAX_BODY_CHARS, CALENDAR_ID, MAILBOX };

	protected AbstractGetEventReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID);
	}

	/**
	 * Read one event.
	 *
	 * @param user       the signed in user
	 * @param calendarId the calendar holding it, or null for the default one
	 * @param mailbox    whose calendar, or null for the user's own
	 * @param id         the event
	 * @return the event
	 * @throws Exception when the event cannot be read
	 */
	protected abstract CalendarEvent getEvent(User user, String calendarId, String mailbox, String id) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("read the calendar event", () -> {
			String id = requireEventId("read a calendar event");
			int maxBodyChars = readCount(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);
			return getEvent(this.insight.getUser(), readString(CALENDAR_ID), readString(MAILBOX), id).toMap(true,
					maxBodyChars);
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return "calendar/event";
	}

	@Override
	protected final String describe() {
		return "Read one event from a " + calendar() + ", the signed in user's own or one shared or delegated to them.";
	}

	@Override
	protected final String describeKey(String key) {
		return super.describeKey(key);
	}
}
