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
import prerna.io.connector.ConnectorPage;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the calendars the signed in user can see, which is their own and the
 * ones other people shared with them.
 */
public abstract class AbstractListCalendarsReactor extends AbstractCalendarReactor {

	/** How many calendars come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 100;

	/** The most a caller can ask for at once. */
	private static final int MAX_LIMIT = 500;

	private static final String[] KEYS = { LIMIT, OFFSET, MAILBOX };

	protected AbstractListCalendarsReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet);
	}

	/**
	 * Read one page of calendars.
	 *
	 * @param user    the signed in user
	 * @param mailbox whose calendars, or null for the user's own
	 * @param offset  how many calendars to skip
	 * @param limit   how many calendars the page holds
	 * @return the page
	 * @throws Exception when the calendars cannot be read
	 */
	protected abstract ConnectorPage<CalendarInfo> listCalendars(User user, String mailbox, int offset, int limit)
			throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("list your calendars", () -> {
			int offset = readOffset();
			ConnectorPage<CalendarInfo> page = listCalendars(this.insight.getUser(), readString(MAILBOX), offset,
					readCount(LIMIT, DEFAULT_LIMIT, MAX_LIMIT));

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(OFFSET, offset);
			output.put("count", page.items().size());
			output.put("hasMore", page.hasMore());
			output.put("calendars", page.items().stream().map(CalendarInfo::toMap).toList());
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return null;
	}

	@Override
	protected final String describe() {
		return "List the " + calendar()
				+ " calendars of the signed in user, their own and the ones other people shared with them.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case LIMIT:
			return "Optional number of calendars to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		case MAILBOX:
			return "Optional email address of somebody who shared their calendar with the signed in user, to list "
					+ "that person's calendars instead.";
		default:
			return super.describeKey(key);
		}
	}
}
