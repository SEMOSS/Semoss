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

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Deletes an event. One the calendar's owner organized is canceled for
 * everybody invited, and one they were invited to is only taken off their own
 * calendar.
 */
public abstract class AbstractDeleteEventReactor extends AbstractCalendarReactor {

	private static final String[] KEYS = { ID, CALENDAR_ID, MAILBOX };

	protected AbstractDeleteEventReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID);
	}

	/**
	 * Delete the event.
	 *
	 * @param user       the signed in user
	 * @param calendarId the calendar holding it, or null for the default one
	 * @param mailbox    whose calendar, or null for the user's own
	 * @param id         the event
	 * @throws Exception when the event cannot be deleted
	 */
	protected abstract void deleteEvent(User user, String calendarId, String mailbox, String id) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("delete the calendar event", () -> {
			String id = requireEventId("delete a calendar event");
			deleteEvent(this.insight.getUser(), readString(CALENDAR_ID), readString(MAILBOX), id);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(ID, id);
			output.put("deleted", true);
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected final String getView() {
		return "calendar/event?intent=delete";
	}

	@Override
	protected final String describe() {
		return "Delete an event from a " + calendar()
				+ ", the signed in user's own or one shared with them to write, canceling it for the attendees when "
				+ "the calendar's owner organized it.";
	}

	@Override
	protected final String describeKey(String key) {
		return super.describeKey(key);
	}
}
