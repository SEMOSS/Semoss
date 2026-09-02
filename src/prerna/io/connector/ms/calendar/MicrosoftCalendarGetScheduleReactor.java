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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads when people are free and when they are busy.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.Read.Shared} for
 * {@code POST /me/calendar/getSchedule}</li>
 * </ul>
 * 
 * <p>
 * This is how a caller finds a time that suits everybody without reading
 * anybody's calendar. Each mailbox comes back with an availability view, a
 * string of one digit per slot of the window where 0 is free, and with the
 * items behind it only as far as that mailbox has chosen to share them.
 * </p>
 *
 * <p>
 * A mailbox that will not be read comes back with the reason under
 * {@code error} rather than failing the whole call, so asking about somebody
 * outside the tenant does not cost the answer for everybody else.
 * </p>
 */
public class MicrosoftCalendarGetScheduleReactor extends AbstractMicrosoftCalendarReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarGetScheduleReactor.class);

	private static final String SCHEDULES = "schedules";
	private static final String DAYS = "days";
	private static final String INTERVAL = "availabilityViewInterval";

	/** How far ahead the window reaches when a caller does not say. */
	private static final int DEFAULT_DAYS = 1;

	/** How many minutes each slot covers when a caller does not say. */
	private static final int DEFAULT_INTERVAL = 30;

	/** The widest and narrowest slot Graph will divide a window into. */
	private static final int MAX_INTERVAL = 1440;
	private static final int MIN_INTERVAL = 5;

	public MicrosoftCalendarGetScheduleReactor() {
		this.keysToGet = new String[] { SCHEDULES, START, END, DAYS, TIME_ZONE, INTERVAL };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String[] schedules = values(SCHEDULES);
		if (schedules == null) {
			throw new SemossPixelException("At least one email address is required under " + SCHEDULES + ".");
		}
		String timeZone = trimToNull(this.keyValue.get(TIME_ZONE));
		int days = positiveInt(DAYS, DEFAULT_DAYS, Integer.MAX_VALUE);
		int interval = positiveInt(INTERVAL, DEFAULT_INTERVAL, MAX_INTERVAL);
		if (interval < MIN_INTERVAL) {
			throw new SemossPixelException(INTERVAL + " must be at least " + MIN_INTERVAL + " minutes.");
		}

		// the window starts now and runs a day unless the caller frames it, which is
		// what somebody asking who is free means
		String start = trimToNull(this.keyValue.get(START));
		if (start == null) {
			start = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
		}
		String end = trimToNull(this.keyValue.get(END));

		try {
			if (end == null) {
				end = MicrosoftCalendarHelper.plusDays(start, days);
			}

			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			List<Map<String, Object>> entries = MicrosoftCalendarHelper.getSchedule(accessToken,
					Arrays.asList(schedules), start, end, timeZone, interval);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("start", start);
			output.put("end", end);
			output.put(INTERVAL, interval);
			output.put(SCHEDULES, entries);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading the schedule of {}", Arrays.toString(schedules), e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read the schedule of {}", Arrays.toString(schedules), e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read the schedule of {}", Arrays.toString(schedules), e);
			throw new SemossPixelException("An error occurred reading the schedule. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read the free and busy times of one or more Microsoft 365 mailboxes, to find a time that suits everybody.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SCHEDULES)) {
			return "Email addresses of the mailboxes to look at, passed as several values or as one comma separated value.";
		} else if (key.equals(START)) {
			return "Optional start of the window as an ISO 8601 date and time. Defaults to now.";
		} else if (key.equals(END)) {
			return "Optional end of the window as an ISO 8601 date and time. Defaults to the number of days after the start.";
		} else if (key.equals(DAYS)) {
			return "Optional number of days the window covers when no end is given. Defaults to " + DEFAULT_DAYS + ".";
		} else if (key.equals(INTERVAL)) {
			return "Optional number of minutes each slot of the availability view covers. Defaults to "
					+ DEFAULT_INTERVAL + " and must be between " + MIN_INTERVAL + " and " + MAX_INTERVAL + ".";
		}
		return super.getDescriptionForKey(key);
	}
}
