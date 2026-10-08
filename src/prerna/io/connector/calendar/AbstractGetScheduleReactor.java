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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ConnectorTimes;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads when one or more people are busy, to find a time that suits everybody
 * without reading anybody's events.
 */
public abstract class AbstractGetScheduleReactor extends AbstractCalendarReactor {

	/** How far ahead the window reaches when a caller does not say. */
	private static final int DEFAULT_DAYS = 1;

	private static final String[] KEYS = { SCHEDULES, START, END, DAYS, TIME_ZONE };

	protected AbstractGetScheduleReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, SCHEDULES);
	}

	/**
	 * Read when each person is busy over the window.
	 *
	 * @param user      the signed in user
	 * @param schedules the people, by email address
	 * @param start     the start of the window
	 * @param end       the end of the window
	 * @return one entry for each person asked about
	 * @throws Exception when the schedules cannot be read
	 */
	protected abstract List<Availability> getSchedule(User user, List<String> schedules, Instant start, Instant end)
			throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("read the schedule", () -> {
			List<String> schedules = readValues(SCHEDULES);
			if (schedules == null) {
				throw new SemossPixelException("At least one email address is required under " + SCHEDULES + ".");
			}
			Instant[] window = readWindow(DEFAULT_DAYS);
			List<Availability> availability = getSchedule(this.insight.getUser(), schedules, window[0], window[1]);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(START, ConnectorTimes.format(window[0]));
			output.put(END, ConnectorTimes.format(window[1]));
			output.put(SCHEDULES, availability.stream().map(Availability::toMap).toList());
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return "calendar/availability";
	}

	@Override
	protected final String describe() {
		return "Read when one or more people are busy on their " + calendar()
				+ ", to find a time that suits everybody.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case SCHEDULES:
			return "Email addresses of the people to look at, as a list.";
		case START:
			return "Optional start of the window as an ISO 8601 date and time. Defaults to now.";
		case END:
			return "Optional end of the window as an ISO 8601 date and time. Defaults to the number of days after the start.";
		case DAYS:
			return "Optional number of days the window covers when no end is given. Defaults to " + DEFAULT_DAYS + ".";
		default:
			return super.describeKey(key);
		}
	}
}
