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
package prerna.io.connector;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

/**
 * How the mail and calendar reactors read and write times.
 *
 * <p>
 * Every time a reactor answers with is UTC, written as ISO 8601 with a Z, and a
 * whole day is written as a date. A time a caller passes may carry an offset or
 * a Z, or may be a local time, which is read in the zone the caller names, or
 * in UTC when they name none.
 * </p>
 */
public final class ConnectorTimes {

	private ConnectorTimes() {

	}

	/**
	 * @param instant the moment
	 * @return the moment as ISO 8601 in UTC, to the second, or null when there is
	 *         none
	 */
	public static String format(Instant instant) {
		if (instant == null) {
			return null;
		}
		return DateTimeFormatter.ISO_INSTANT.format(instant.truncatedTo(ChronoUnit.SECONDS));
	}

	/**
	 * Read a time a provider answered with.
	 *
	 * <p>
	 * Gmail and Google Calendar write an offset, and Graph writes a local time that
	 * is in UTC unless it was asked for another zone, which these reactors never
	 * do, so a value without an offset is read as UTC here.
	 * </p>
	 *
	 * @param value the time as the provider wrote it
	 * @return the moment, or null when there is none or it cannot be read
	 */
	public static Instant parseProviderTime(String value) {
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		String trimmed = value.trim();
		try {
			return OffsetDateTime.parse(trimmed).toInstant();
		} catch (DateTimeParseException e) {
			// no offset, which is how Graph writes a time it was not asked to zone
		}
		try {
			return LocalDateTime.parse(trimmed).toInstant(ZoneOffset.UTC);
		} catch (DateTimeParseException e) {
			return null;
		}
	}

	/**
	 * Read a time a caller passed.
	 *
	 * @param value a date and time with an offset or a Z, a local date and time, or
	 *              a date, which is read as the start of that day
	 * @param zone  the zone a local value is read in
	 * @return the moment
	 * @throws IllegalArgumentException if the value cannot be read
	 */
	public static Instant readMoment(String value, ZoneId zone) {
		String trimmed = value == null ? "" : value.trim();
		try {
			return OffsetDateTime.parse(trimmed).toInstant();
		} catch (DateTimeParseException e) {
			// no offset, so the caller meant the zone they named
		}
		try {
			return LocalDateTime.parse(trimmed).atZone(zone).toInstant();
		} catch (DateTimeParseException e) {
			// no time either, so a whole date
		}
		try {
			return LocalDate.parse(trimmed).atStartOfDay(zone).toInstant();
		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException(
					"Times must be ISO 8601, such as 2026-09-01T13:00:00Z or 2026-09-01T13:00:00, but received: "
							+ value);
		}
	}

	/**
	 * Read a date a caller passed, for a whole day event or the last day a series
	 * repeats on.
	 *
	 * @param value a date, or a date and time whose date is taken
	 * @return the date
	 * @throws IllegalArgumentException if the value cannot be read
	 */
	public static LocalDate readDate(String value) {
		String trimmed = value == null ? "" : value.trim();
		try {
			return LocalDate.parse(trimmed);
		} catch (DateTimeParseException e) {
			// a date and time, whose date is what counts
		}
		try {
			return OffsetDateTime.parse(trimmed).toLocalDate();
		} catch (DateTimeParseException e) {
			// no offset
		}
		try {
			return LocalDateTime.parse(trimmed).toLocalDate();
		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException("Dates must be ISO 8601, such as 2026-09-01, but received: " + value);
		}
	}

	/**
	 * The name a provider takes for a zone. Java names UTC {@code Z}, which neither
	 * Graph nor Google reads as a zone, so UTC is written out by name.
	 *
	 * @param zone the zone
	 * @return the zone's name, such as America/New_York or UTC
	 */
	public static String providerZoneId(ZoneId zone) {
		if (zone == null || zone.normalized().equals(ZoneOffset.UTC)) {
			return "UTC";
		}
		return zone.getId();
	}

	/**
	 * Read the zone a caller named.
	 *
	 * @param value an IANA zone such as America/New_York, or null for UTC
	 * @return the zone
	 * @throws IllegalArgumentException if the value is not an IANA zone
	 */
	public static ZoneId readZone(String value) {
		if (value == null || value.trim().isEmpty()) {
			return ZoneOffset.UTC;
		}
		try {
			return ZoneId.of(value.trim());
		} catch (DateTimeException e) {
			throw new IllegalArgumentException(
					"The time zone must be an IANA zone such as America/New_York, but received: " + value);
		}
	}
}
