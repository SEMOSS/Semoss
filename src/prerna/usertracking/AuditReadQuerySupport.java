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
package prerna.usertracking;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;

/**
 * Shared query mechanics for the readers of the audit trail; each reader owns
 * its authorization and allowed fields. Readers build a
 * {@link SelectQueryStruct} on {@link UserAuditTrailUtils#TABLE} and run it
 * with the structured {@code queryList} of
 * {@link prerna.util.QueryExecutionUtility}, so every value is bound and each
 * column keeps its own type.
 */
public final class AuditReadQuerySupport {

	/** The longest read of the audit trail, in seconds */
	public static final int QUERY_TIMEOUT_SECONDS = 30;

	private AuditReadQuerySupport() {
	}

	/**
	 * Names a column of the audit trail the way a query struct does.
	 *
	 * @param name the column, such as {@code EVENT_TIME}
	 * @return {@code USER_AUDIT_EVENTS__<name>}
	 */
	public static String column(String name) {
		return UserAuditTrailUtils.TABLE + "__" + name;
	}

	/**
	 * The names the audit trail stores values by, for a filter on them.
	 *
	 * @param values event types, target types or other audit values
	 * @return their names, in the collection's order
	 */
	public static List<String> names(Collection<? extends Enum<?>> values) {
		return values.stream().map(Enum::name).toList();
	}

	/**
	 * Adds an exact match for each allowed filter the options give a value for.
	 *
	 * @param qs            the read to filter
	 * @param allowedFields the option names a reader accepts, by the column each
	 *                      matches
	 * @param options       the reader's options, with blank values ignored
	 * @param lengthError   the message for a value longer than 255 characters
	 */
	static void addExactFilters(SelectQueryStruct qs, Map<String, String> allowedFields, Map<String, String> options,
			String lengthError) {
		for (var field : allowedFields.entrySet()) {
			String value = options.get(field.getKey());
			if (value != null && !value.isBlank()) {
				if (value.length() > 255) {
					throw new IllegalArgumentException(lengthError);
				}
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column(field.getValue()), "==", value));
			}
		}
	}

	/**
	 * Keeps a read to the events recorded on and between the options' {@code from}
	 * and {@code to} dates, in UTC; either can be left out.
	 *
	 * @param qs         the read to filter
	 * @param options    the reader's options
	 * @param dateError  the message for a date that is not a real
	 *                   {@code YYYY-MM-DD}
	 * @param rangeError the message for a {@code from} after the {@code to}
	 */
	static void addDateFilters(SelectQueryStruct qs, Map<String, String> options, String dateError, String rangeError) {
		LocalDate from = date(options.get("from"), dateError);
		LocalDate to = date(options.get("to"), dateError);
		if (from != null && to != null && from.isAfter(to)) {
			throw new IllegalArgumentException(rangeError);
		}
		addTimeRange(qs, from == null ? null : Timestamp.valueOf(from.atStartOfDay()),
				to == null ? null : Timestamp.valueOf(to.plusDays(1).atStartOfDay()));
	}

	/**
	 * Keeps a read to the events recorded at or after the start and before the end,
	 * both UTC times as the audit trail stores them; either can be null for no
	 * bound.
	 *
	 * @param qs    the read to filter
	 * @param start the first time included, or null
	 * @param end   the first time left out, or null
	 */
	public static void addTimeRange(SelectQueryStruct qs, Timestamp start, Timestamp end) {
		if (start != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column("EVENT_TIME"), ">=", start,
					PixelDataType.CONST_TIMESTAMP));
		}
		if (end != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column("EVENT_TIME"), "<", end,
					PixelDataType.CONST_TIMESTAMP));
		}
	}

	/**
	 * Reads a time the audit trail stores, a UTC time with no zone, as an instant.
	 *
	 * @param value the stored time, or null
	 * @return the ISO-8601 instant, such as {@code 2026-10-13T23:59:59.999Z}, or
	 *         null
	 */
	public static String utcTimestamp(Timestamp value) {
		return value == null ? null : value.toLocalDateTime().toInstant(ZoneOffset.UTC).toString();
	}

	private static LocalDate date(String value, String error) {
		if (value == null || value.isBlank()) {
			return null;
		}
		try {
			if (value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
				return LocalDate.parse(value);
			}
		} catch (java.time.DateTimeException ignored) {
		}
		throw new IllegalArgumentException(error);
	}
}
