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
 * Changes an event, leaving whatever is not passed as it was.
 */
public abstract class AbstractUpdateEventReactor extends AbstractEventWriteReactor {

	protected AbstractUpdateEventReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(withExtraKeys(new String[] { ID }, EVENT_KEYS), extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID);
	}

	/**
	 * Change the event, writing only what the request sets.
	 *
	 * @param user    the signed in user
	 * @param request the changes, with the event's id
	 * @return the event as the calendar left it
	 * @throws Exception when the event cannot be changed
	 */
	protected abstract CalendarEvent updateEvent(User user, EventRequest request) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("change the calendar event", () -> {
			String id = requireEventId("change a calendar event");
			EventRequest request = readEvent(id, false, "change on the calendar event");
			return updateEvent(this.insight.getUser(), request).toMap(true, DEFAULT_MAX_BODY_CHARS);
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected final String getView() {
		return "calendar/event-edit?intent=update";
	}

	@Override
	protected final String describe() {
		return "Change an event on a " + calendar()
				+ ", the signed in user's own or one shared with them to write, leaving whatever is not passed as it was.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case ATTENDEES:
			return "Optional email addresses of the required attendees, as a list. Passing any attendees replaces "
					+ "the whole guest list, so pass everybody who should be on it.";
		case OPTIONAL_ATTENDEES:
			return "Optional email addresses of the optional attendees, as a list. Passing any attendees replaces "
					+ "the whole guest list, so pass everybody who should be on it.";
		default:
			return super.describeKey(key);
		}
	}
}
