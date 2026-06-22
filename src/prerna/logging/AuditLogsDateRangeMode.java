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
package prerna.logging;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import prerna.date.SemossDate;

public enum AuditLogsDateRangeMode {
	DAY, WEEK, MONTH, CUSTOM;

	private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

	public static AuditLogsDateRangeMode from(String s) {
		if (s == null) {
			return MONTH;
		}
		try {
			return AuditLogsDateRangeMode.valueOf(s.trim().toUpperCase());
		} catch (IllegalArgumentException e) {
			return MONTH;
		}
	}

	/**
	 * Resolve the start (and, for custom, end) timestamp bounds for an audit logs
	 * query. For {@link #CUSTOM}, {@code startDateCustom} and {@code endDateCustom}
	 * (ISO-8601 instants) are required and start must be before end. For
	 * DAY/WEEK/MONTH the start is "now minus N units" in UTC with no explicit end.
	 *
	 * @param mode            the date range mode
	 * @param dateRangeValue  number of units back (ignored for custom; coerced to 1
	 *                        when &lt;= 0)
	 * @param startDateCustom custom start (required when mode is CUSTOM)
	 * @param endDateCustom   custom end (required when mode is CUSTOM)
	 * @return a map with {@link SemossLogUtils#START_DATE} (always) and
	 *         {@link SemossLogUtils#END_DATE} (custom only)
	 */
	/**
	 * Resolve the date range while guaranteeing the (large) audit logs table is
	 * never queried unbounded. When the query is not otherwise narrowed
	 * ({@code queryIsBounded == false}, e.g. no roomId) and no explicit date range
	 * was supplied, this defaults to a single day rather than the broader monthly
	 * default - so a project/engine-wide call can never trigger a full-table scan.
	 *
	 * @param dateRangeType   raw mode string (null/blank means "not provided")
	 * @param dateRangeValue  number of units back
	 * @param startDateCustom custom start (required when type is custom)
	 * @param endDateCustom   custom end (required when type is custom)
	 * @param queryIsBounded  whether the query is already constrained by something
	 *                        other than the date range (e.g. a roomId)
	 * @return the resolved date range
	 */
	public static Map<String, SemossDate> resolveDateRange(String dateRangeType, int dateRangeValue,
			String startDateCustom, String endDateCustom, boolean queryIsBounded) {
		boolean explicitRange = dateRangeType != null && !dateRangeType.isBlank();
		AuditLogsDateRangeMode mode = (!explicitRange && !queryIsBounded) ? DAY : from(dateRangeType);
		return resolveDateRange(mode, dateRangeValue, startDateCustom, endDateCustom);
	}

	public static Map<String, SemossDate> resolveDateRange(AuditLogsDateRangeMode mode, int dateRangeValue,
			String startDateCustom, String endDateCustom) {
		if (mode == CUSTOM) {
			// validate inputs
			if (startDateCustom == null || endDateCustom == null) {
				throw new IllegalArgumentException("For custom mode, startDate and endDate are required.");
			}
			// convert start and end UTC date strings to Instant
			Instant startInstant = Instant.parse(startDateCustom);
			Instant endInstant = Instant.parse(endDateCustom);

			if (!startInstant.isBefore(endInstant)) {
				throw new IllegalArgumentException("Start date must be before End date");
			}

			return Map.of(SemossLogUtils.START_DATE, new SemossDate(startInstant, UTC_ZONE), SemossLogUtils.END_DATE,
					new SemossDate(endInstant, UTC_ZONE));
		}

		// non-custom modes
		dateRangeValue = (dateRangeValue <= 0) ? 1 : dateRangeValue;

		ZonedDateTime currentDateTime = ZonedDateTime.now(UTC_ZONE);
		ZonedDateTime targetDateTime;
		switch (mode) {
		case DAY:
			targetDateTime = currentDateTime.minusDays(dateRangeValue);
			break;
		case WEEK:
			targetDateTime = currentDateTime.minusWeeks(dateRangeValue);
			break;
		case MONTH:
		default:
			targetDateTime = currentDateTime.minusMonths(dateRangeValue);
			break;
		}

		return Map.of(SemossLogUtils.START_DATE, new SemossDate(targetDateTime));
	}
}
