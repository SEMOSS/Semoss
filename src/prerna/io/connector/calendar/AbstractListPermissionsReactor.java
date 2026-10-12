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
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ConnectorOutput;
import prerna.io.connector.ConnectorPage;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads who a calendar is shared with, and what each of them may do with it.
 */
public abstract class AbstractListPermissionsReactor extends AbstractCalendarReactor {

	/** How many permissions come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 100;

	/** The most a caller can ask for at once. */
	private static final int MAX_LIMIT = 500;

	private static final String[] KEYS = { CALENDAR_ID, MAILBOX, LIMIT, OFFSET };

	protected AbstractListPermissionsReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet);
	}

	/**
	 * Read every permission on the calendar.
	 *
	 * @param user       the signed in user
	 * @param calendarId the calendar, or null for the default one
	 * @param mailbox    whose calendar, or null for the user's own
	 * @return the permissions
	 * @throws Exception when they cannot be read, which includes the user not being
	 *                   allowed to see them
	 */
	protected abstract List<CalendarPermission> listPermissions(User user, String calendarId, String mailbox)
			throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("read the calendar permissions", () -> {
			String calendarId = readString(CALENDAR_ID);
			String mailbox = readString(MAILBOX);
			int offset = readOffset();
			List<CalendarPermission> permissions = listPermissions(this.insight.getUser(), calendarId, mailbox);
			ConnectorPage<CalendarPermission> page = ConnectorPage.of(permissions, offset,
					readCount(LIMIT, DEFAULT_LIMIT, MAX_LIMIT));

			Map<String, Object> output = new LinkedHashMap<>();
			ConnectorOutput.putIfPresent(output, CALENDAR_ID, calendarId);
			ConnectorOutput.putIfPresent(output, MAILBOX, mailbox);
			output.put(OFFSET, offset);
			output.put("count", page.items().size());
			output.put("hasMore", page.hasMore());
			output.put("permissions", page.items().stream().map(CalendarPermission::toMap).toList());
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
		return "Read who a " + calendar()
				+ " is shared with, and whether each of them is a reader, a writer or a delegate.";
	}

	@Override
	protected final String describeKey(String key) {
		if (LIMIT.equals(key)) {
			return "Optional number of permissions to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		}
		return super.describeKey(key);
	}
}
