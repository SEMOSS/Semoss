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
package prerna.auth.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditReadQuerySupport;
import prerna.usertracking.AuditStatus;
import prerna.usertracking.AuditTargetCatalog;
import prerna.util.QueryExecutionUtility;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;
import prerna.util.QueryExecutionUtility.RowMapper;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Fixed, parameterized analytics over the internal logging databases and the
 * audit trail's access changes, denied requests and usage limit changes.
 */
public final class EnterpriseUsageUtils {

	private static final Logger classLogger = LogManager.getLogger(EnterpriseUsageUtils.class);

	public static final int MAX_LIMIT = 5000;
	/** The longest read of any source, in seconds */
	private static final int QUERY_TIMEOUT_SECONDS = 30;

	/** The usage limit changes the audit source reads */
	private static final Set<AuditEventType> USAGE_LIMIT_EVENTS = EnumSet.of(AuditEventType.USAGE_LIMIT_UPDATE);
	/**
	 * The access changes the audit source reads: access given, changed, ended or
	 * taken away, and access requests answered
	 */
	private static final Set<AuditEventType> ACCESS_EVENTS = EnumSet.of(AuditEventType.PERMISSION_ADD,
			AuditEventType.PERMISSION_UPDATE, AuditEventType.PERMISSION_DELETE, AuditEventType.PERMISSION_BULK_GRANT,
			AuditEventType.PERMISSION_BATCH_APPLY, AuditEventType.ACCESS_REQUEST_APPROVE,
			AuditEventType.ACCESS_REQUEST_REJECT);
	/** Every change the audit source reads */
	private static final Set<AuditEventType> CHANGE_EVENTS = Stream
			.concat(USAGE_LIMIT_EVENTS.stream(), ACCESS_EVENTS.stream())
			.collect(Collectors.toCollection(() -> EnumSet.noneOf(AuditEventType.class)));
	/**
	 * What the audit source's logs show of each event, with the action a denied
	 * request tried; its old and new values and its error are read one event at a
	 * time, as its detail
	 */
	private static final List<String> AUDIT_LOG_COLUMNS = List.of("EVENT_ID", "EVENT_TIME", "EVENT_TYPE", "ACTION",
			"STATUS", "SEVERITY", "ACTOR_USER_ID", "ACTOR_USER_TYPE", "ACTOR_USER_NAME", "SUBJECT_USER_ID",
			"SUBJECT_USER_TYPE", "SUBJECT_USER_NAME", "TARGET_TYPE", "TARGET_ID", "TARGET_NAME", "PROJECT_ID",
			"ENGINE_ID");
	/**
	 * An audit event's change, or why its request was denied, as its detail shows
	 * it
	 */
	private static final List<String> AUDIT_DETAIL_COLUMNS = List.of("EVENT_ID", "OLD_VALUE", "NEW_VALUE", "DETAILS",
			"ERROR_CODE", "ERROR_MESSAGE");
	/** The counts of the audit source's summary */
	private static final List<String> AUDIT_METRICS = List.of("EVENTS", "USAGE_LIMIT_CHANGES", "ACCESS_CHANGES",
			"DENIALS", "ACTORS", "PEOPLE_DENIED", "PEOPLE_AFFECTED");

	private static final String MODEL_FROM = """
			FROM MESSAGE m
			 LEFT JOIN (SELECT ROOM_ID, MAX(PROJECT_ID) AS PROJECT_ID, MAX(PROJECT_NAME) AS PROJECT_NAME FROM ROOM GROUP BY ROOM_ID) r ON m.ROOM_ID = r.ROOM_ID
			 LEFT JOIN (SELECT AGENT_ID, MAX(AGENT_NAME) AS AGENT_NAME FROM AGENT GROUP BY AGENT_ID) a ON m.AGENT_ID = a.AGENT_ID
			        """;

	private static final String MODEL_METRICS = """
			COALESCE(SUM(CASE WHEN m.MESSAGE_TYPE = 'INPUT' THEN 1 ELSE 0 END), 0) AS REQUESTS,
			 COUNT(DISTINCT m.USER_ID) AS USERS, COUNT(DISTINCT r.PROJECT_ID) AS APPS,
			 COUNT(DISTINCT m.AGENT_ID) AS MODELS, COUNT(DISTINCT m.ROOM_ID) AS ROOMS,
			 CASE WHEN COUNT(CASE WHEN m.MESSAGE_TYPE = 'INPUT' THEN 1 END) = 0 THEN 0 ELSE SUM(CASE WHEN m.MESSAGE_TYPE = 'INPUT' THEN m.MESSAGE_TOKENS END) END AS INPUT_TOKENS,
			 CASE WHEN COUNT(CASE WHEN m.MESSAGE_TYPE = 'RESPONSE' THEN 1 END) = 0 THEN 0 ELSE SUM(CASE WHEN m.MESSAGE_TYPE = 'RESPONSE' THEN m.MESSAGE_TOKENS END) END AS OUTPUT_TOKENS,
			 CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(m.MESSAGE_TOKENS) END AS TOKENS,
			 COUNT(m.MESSAGE_TOKENS) AS TOKEN_ROWS, COUNT(*) AS MESSAGE_ROWS,
			 COALESCE(SUM(m.CACHE_READ_TOKENS), 0) AS CACHE_READ_TOKENS,
			 COUNT(m.CACHE_READ_TOKENS) AS CACHE_ROWS,
			 SUM(m.CACHE_CREATION_TOKENS) AS CACHE_CREATION_TOKENS,
			 COALESCE(SUM(m.THINKING_TOKENS), 0) AS THINKING_TOKENS,
			 COUNT(m.THINKING_TOKENS) AS THINKING_ROWS,
			 AVG(CASE WHEN m.MESSAGE_TYPE = 'RESPONSE' AND m.RESPONSE_TIME >= 0 THEN m.RESPONSE_TIME ELSE NULL END) AS LATENCY_MS
			        """;

	private static final String ACTIVITY_METRICS = """
			COUNT(*) AS EVENTS, COUNT(DISTINCT l.USER_ID) AS USERS,
			 COUNT(DISTINCT l.PROJECT_ID) AS APPS,
			 COALESCE(SUM(CASE WHEN l.IS_SUCCESS = TRUE THEN 1 ELSE 0 END), 0) AS SUCCEEDED,
			 COALESCE(SUM(CASE WHEN l.IS_SUCCESS = FALSE THEN 1 ELSE 0 END), 0) AS FAILED,
			 COUNT(l.IS_SUCCESS) AS KNOWN_OUTCOMES
			        """;

	/**
	 * Where a report reads from: model inference logs, platform activity logs, or
	 * the audit trail's access changes, denied requests and usage limit changes
	 */
	public enum Source {
		MODEL, ACTIVITY, AUDIT
	}

	public enum View {
		SUMMARY, TREND, LATENCY, FEEDBACK, RANKING, LOGS
	}

	public enum Dimension {
		USER, APP, MODEL
	}

	/** Catalog entities available to the enterprise filter selectors. */
	public enum FilterDimension {
		USER, APP, ENGINE
	}

	private EnterpriseUsageUtils() {
	}

	/**
	 * The times an inclusive range of calendar dates covers, from the start of its
	 * first day up to the start of the day after its last, as times with no zone in
	 * the server's time zone, the way the activity logs keep them.
	 *
	 * @param start the first time included
	 * @param end   the first time left out
	 */
	private record Period(Timestamp start, Timestamp end) {

		/**
		 * The same period as UTC times with no zone, the way the audit trail keeps its
		 * times.
		 *
		 * @param zone the time zone the period's days are in
		 * @return the period's bounds in UTC
		 */
		Period inUtc(ZoneId zone) {
			return new Period(utc(start, zone), utc(end, zone));
		}

		private static Timestamp utc(Timestamp time, ZoneId zone) {
			return Timestamp
					.valueOf(time.toLocalDateTime().atZone(zone).withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime());
		}
	}

	/** Strictly parses the finite source/view/dimension allowlists. */
	public static <T extends Enum<T>> T option(Class<T> type, String value) {
		try {
			return Enum.valueOf(type, value.toUpperCase(Locale.ROOT));
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Invalid " + type.getSimpleName());
		}
	}

	/** Rejects invalid pagination instead of permitting unbounded collection. */
	public static int boundedInteger(String value, int fallback, int min, int max) {
		try {
			int parsed = value == null || value.isBlank() ? fallback : Integer.parseInt(value);
			if (parsed < min || parsed > max) {
				throw new NumberFormatException();
			}
			return parsed;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid pagination value");
		}
	}

	private static String text(String value) {
		if (value == null) {
			return "";
		}
		if (value.length() > 256 || value.chars().anyMatch(Character::isISOControl)) {
			throw new IllegalArgumentException(
					"Filters and identifiers must be at most 256 characters without control characters");
		}
		return value;
	}

	private static void search(StringBuilder sql, List<Object> params, String id, String name, String value) {
		value = text(value);
		if (value.isBlank()) {
			return;
		}
		if (value.startsWith("=")) {
			sql.append(" AND ").append(id).append(" = ?");
			params.add(value.substring(1));
		} else {
			sql.append(" AND (LOWER(").append(id).append(") LIKE ? ESCAPE '!' OR LOWER(").append(name)
					.append(") LIKE ? ESCAPE '!')");
			String match = "%"
					+ value.trim().toLowerCase(Locale.ROOT).replace("!", "!!").replace("%", "!%").replace("_", "!_")
					+ "%";
			params.add(match);
			params.add(match);
		}
	}

	/**
	 * Reads a report's dates.
	 *
	 * @return the times the dates cover
	 * @throws IllegalArgumentException unless both are real {@code YYYY-MM-DD}
	 *                                  dates, the end on or after the start, at
	 *                                  most 366 days in all
	 */
	private static Period period(String from, String to) {
		LocalDate start;
		LocalDate end;
		try {
			if (from == null || to == null || !from.matches("\\d{4}-\\d{2}-\\d{2}")
					|| !to.matches("\\d{4}-\\d{2}-\\d{2}")) {
				throw new IllegalArgumentException();
			}
			start = LocalDate.parse(from);
			end = LocalDate.parse(to);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Valid startDate and endDate are required");
		}
		long days = ChronoUnit.DAYS.between(start, end);
		if (days < 0 || days > 365) {
			throw new IllegalArgumentException("Date range must be between 1 and 366 inclusive calendar days");
		}
		return new Period(Timestamp.valueOf(start.atStartOfDay()), Timestamp.valueOf(end.plusDays(1).atStartOfDay()));
	}

	/** Rejects a page beyond the bounds every source allows. */
	private static void pagination(int limit, int offset) {
		if (limit < 1 || limit > MAX_LIMIT || offset < 0 || offset > Integer.MAX_VALUE - MAX_LIMIT) {
			throw new IllegalArgumentException("Invalid pagination value");
		}
	}

	private static String scope(Source source, String from, String to, String user, String app, String engineFilter,
			List<Object> params) {
		Period period = period(from, to);
		String time = source == Source.MODEL ? "m.DATE_CREATED" : "l.LOG_TIMESTAMP";
		StringBuilder sql = new StringBuilder(source == Source.MODEL ? MODEL_FROM : "FROM AUDIT_LOGS l");
		sql.append(" WHERE ").append(time).append(" >= ? AND ").append(time).append(" < ?");
		params.add(period.start());
		params.add(period.end());
		if (source == Source.MODEL) {
			sql.append(
					" AND m.MESSAGE_TYPE IN ('INPUT', 'RESPONSE') AND (m.MESSAGE_METHOD IS NULL OR m.MESSAGE_METHOD <> 'nearestNeighbor')");
			search(sql, params, "m.USER_ID", "m.USER_NAME", user);
			search(sql, params, "r.PROJECT_ID", "r.PROJECT_NAME", app);
			search(sql, params, "m.AGENT_ID", "a.AGENT_NAME", engineFilter);
		} else {
			search(sql, params, "l.USER_ID", "l.USER_NAME", user);
			search(sql, params, "l.PROJECT_ID", "l.PROJECT_NAME", app);
			search(sql, params, "l.ENGINE_ID", "l.ENGINE_NAME", engineFilter);
		}
		return sql.toString();
	}

	/**
	 * Builds a report on the model or activity logs as fixed SQL; the audit source
	 * is read with {@link #auditQuery}.
	 */
	static ParameterizedQuery reportQuery(Source source, View view, String from, String to, String user, String app,
			String engineFilter, String dimension, int limit, int offset) {
		pagination(limit, offset);
		if (source == Source.AUDIT) {
			throw new IllegalArgumentException("The audit trail is read with its own query");
		}
		if (source == Source.ACTIVITY && (view == View.LATENCY || view == View.FEEDBACK || view == View.RANKING)) {
			throw new IllegalArgumentException("This view is available for model usage only");
		}
		List<Object> params = new ArrayList<>();
		String scope = scope(source, from, to, user, app, engineFilter, params);
		String metrics = source == Source.MODEL ? MODEL_METRICS : ACTIVITY_METRICS;
		String sql;
		int bound;
		switch (view) {
		case SUMMARY -> {
			sql = "SELECT " + metrics + " " + scope;
			bound = 1;
		}
		case TREND -> {
			String day = "CAST(" + (source == Source.MODEL ? "m.DATE_CREATED" : "l.LOG_TIMESTAMP") + " AS DATE)";
			sql = "SELECT " + day + " AS DAY, " + metrics + " " + scope + " GROUP BY " + day + " ORDER BY " + day;
			bound = 366;
		}
		case LATENCY -> {
			sql = "SELECT MIN(LATENCY_MS) AS P95_MS FROM (SELECT m.RESPONSE_TIME AS LATENCY_MS, ROW_NUMBER() OVER (ORDER BY m.RESPONSE_TIME) AS RN, COUNT(*) OVER () AS N "
					+ scope
					+ " AND m.MESSAGE_TYPE = 'RESPONSE' AND m.RESPONSE_TIME >= 0) ranked WHERE RN >= CEILING(N * 0.95)";
			bound = 1;
		}
		case FEEDBACK -> {
			sql = "SELECT COUNT(*) AS RATINGS, COALESCE(SUM(f.POSITIVE), 0) AS POSITIVE FROM (SELECT m.MESSAGE_ID, m.MESSAGE_TYPE "
					+ scope
					+ " AND m.MESSAGE_TYPE = 'RESPONSE' GROUP BY m.MESSAGE_ID, m.MESSAGE_TYPE) messages INNER JOIN (SELECT MESSAGE_ID, MESSAGE_TYPE, MAX(CASE WHEN RATING = TRUE THEN 1 ELSE 0 END) AS POSITIVE FROM FEEDBACK WHERE RATING IS NOT NULL GROUP BY MESSAGE_ID, MESSAGE_TYPE) f ON messages.MESSAGE_ID = f.MESSAGE_ID AND messages.MESSAGE_TYPE = f.MESSAGE_TYPE";
			bound = 1;
		}
		case RANKING -> {
			Dimension group = option(Dimension.class, dimension);
			String id = switch (group) {
			case USER -> "m.USER_ID";
			case APP -> "r.PROJECT_ID";
			case MODEL -> "m.AGENT_ID";
			};
			String name = switch (group) {
			case USER -> "m.USER_NAME";
			case APP -> "r.PROJECT_NAME";
			case MODEL -> "a.AGENT_NAME";
			};
			sql = "SELECT " + id + " AS ENTITY_ID, MAX(" + name + ") AS ENTITY_NAME, " + metrics + " " + scope
					+ " GROUP BY " + id + " ORDER BY TOKENS DESC, REQUESTS DESC, " + id;
			bound = Math.min(limit, 100);
		}
		case LOGS -> {
			String columns = source == Source.MODEL
					? "m.DATE_CREATED AS TIME, m.MESSAGE_ID, m.TRANSACTION_ID, m.MESSAGE_TYPE, m.USER_ID, m.USER_NAME, m.AGENT_ID AS MODEL_ID, a.AGENT_NAME AS MODEL_NAME, r.PROJECT_ID AS APP_ID, r.PROJECT_NAME AS APP_NAME, m.ROOM_ID, m.MESSAGE_METHOD AS METHOD, m.MESSAGE_TOKENS AS TOKENS, m.RESPONSE_TIME AS LATENCY_MS"
					: "l.LOG_TIMESTAMP AS TIME, l.LOG_ID, l.REQUEST_ID, l.USER_ID, l.USER_NAME, l.PROJECT_ID AS APP_ID, l.PROJECT_NAME AS APP_NAME, l.ENGINE_ID, l.ENGINE_NAME, l.ENGINE_TYPE, l.ROOM_ID, l.METHOD_NAME AS METHOD, l.IS_SUCCESS, l.LOG_LEVEL";
			String order = source == Source.MODEL
					? "m.DATE_CREATED DESC, m.MESSAGE_ID, m.MESSAGE_TYPE, m.AGENT_ID, m.USER_ID"
					: "l.LOG_TIMESTAMP DESC, l.LOG_ID";
			sql = "SELECT * FROM (SELECT " + columns + ", ROW_NUMBER() OVER (ORDER BY " + order + ") AS ROW_NUM "
					+ scope + ") page_rows WHERE ROW_NUM > ? AND ROW_NUM <= ? ORDER BY ROW_NUM";
			params.add(offset);
			params.add(offset + limit);
			bound = limit;
		}
		default -> throw new IllegalArgumentException("Unsupported view");
		}
		return new ParameterizedQuery(sql, params, bound);
	}

	/** Executes a fixed analytics view, with a capped result and timeout. */
	public static Map<String, Object> report(Source source, View view, String from, String to, String user, String app,
			String engineFilter, String dimension, int limit, int offset) {
		if (source == Source.AUDIT) {
			SelectQueryStruct qs = auditQuery(view, from, to, user, app, engineFilter, limit, offset,
					ZoneId.systemDefault());
			if (view == View.SUMMARY) {
				return execute(source, 1, qs, EnterpriseUsageUtils::auditSummaryRow);
			}
			return execute(source, limit, engine -> {
				List<Map<String, Object>> rows = QueryExecutionUtility.queryList(engine, qs,
						EnterpriseUsageUtils::auditLogRow, QUERY_TIMEOUT_SECONDS);
				// each event's engine or app as the catalog has it now, with what picks its
				// logo
				AuditTargetCatalog.addTo(rows);
				return rows;
			});
		}
		return execute(source, reportQuery(source, view, from, to, user, app, engineFilter, dimension, limit, offset));
	}

	/**
	 * Builds the audit source's summary or a page of its logs: the usage limit
	 * changes, the access changes and the denied requests in the audit trail,
	 * recorded on the report's dates. The dates are days in the server's time zone,
	 * as on the activity logs, and the audit trail keeps UTC times, so the dates'
	 * bounds are read in UTC: a change counts on the server's day it was made, even
	 * when its UTC time falls on another day. A denied request is any event with
	 * the {@code DENIED} status, such as {@code AUTHORIZATION_DENIED} or a denied
	 * audit read. The user filter matches the person who made the change or was
	 * denied, or the person a change was for; the app and engine filters match the
	 * app or engine it was on. As on the logs, {@code =id} matches an exact id and
	 * anything else a fragment of an id or name, case-insensitive.
	 *
	 * @param zone the server's time zone, which the dates are days in
	 * @return a structured read with every value bound
	 * @throws IllegalArgumentException for a view other than summary or logs, or
	 *                                  invalid dates, filters or pagination
	 */
	static SelectQueryStruct auditQuery(View view, String from, String to, String user, String app, String engineFilter,
			int limit, int offset, ZoneId zone) {
		pagination(limit, offset);
		if (view != View.SUMMARY && view != View.LOGS) {
			throw new IllegalArgumentException("The audit trail has a summary and logs only");
		}
		Period period = period(from, to).inUtc(zone);
		SelectQueryStruct qs = new SelectQueryStruct();
		if (view == View.SUMMARY) {
			qs.addSelector(count("EVENTS", new QueryColumnSelector(AuditReadQuerySupport.column("EVENT_ID")), false));
			qs.addSelector(count("USAGE_LIMIT_CHANGES", when(eventTypeIn(USAGE_LIMIT_EVENTS), "EVENT_ID"), false));
			qs.addSelector(count("ACCESS_CHANGES", when(eventTypeIn(ACCESS_EVENTS), "EVENT_ID"), false));
			qs.addSelector(count("DENIALS", when(denied(), "EVENT_ID"), false));
			qs.addSelector(count("ACTORS", when(eventTypeIn(CHANGE_EVENTS), "ACTOR_USER_ID"), true));
			qs.addSelector(count("PEOPLE_DENIED", when(denied(), "ACTOR_USER_ID"), true));
			qs.addSelector(count("PEOPLE_AFFECTED",
					new QueryColumnSelector(AuditReadQuerySupport.column("SUBJECT_USER_ID")), true));
		} else {
			for (String column : AUDIT_LOG_COLUMNS) {
				qs.addSelector(new QueryColumnSelector(AuditReadQuerySupport.column(column)));
			}
			qs.addOrderBy(new QueryColumnOrderBySelector(AuditReadQuerySupport.column("EVENT_TIME"), "DESC"));
			qs.addOrderBy(new QueryColumnOrderBySelector(AuditReadQuerySupport.column("EVENT_ID"), "DESC"));
			qs.setLimit(limit);
			qs.setOffSet(offset);
		}
		AuditReadQuerySupport.addTimeRange(qs, period.start(), period.end());
		qs.addExplicitFilter(auditEvents());
		auditSearch(qs, user, List.of("ACTOR_USER_ID", "SUBJECT_USER_ID"),
				List.of("ACTOR_USER_NAME", "SUBJECT_USER_NAME"));
		auditSearch(qs, app, List.of("PROJECT_ID"), List.of());
		auditSearch(qs, engineFilter, List.of("ENGINE_ID"), List.of());
		return qs;
	}

	/**
	 * Builds the read of what the logs leave out of one audit event: its old and
	 * new values and its details for a change, its error code and message for a
	 * denied request, for an event the audit source shows only.
	 *
	 * @throws IllegalArgumentException without a record id
	 */
	static SelectQueryStruct auditDetailQuery(String recordId) {
		if (text(recordId).isBlank()) {
			throw new IllegalArgumentException("A recordId is required");
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String column : AUDIT_DETAIL_COLUMNS) {
			qs.addSelector(new QueryColumnSelector(AuditReadQuerySupport.column(column)));
		}
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column("EVENT_ID"), "==", recordId));
		qs.addExplicitFilter(auditEvents());
		qs.setLimit(1);
		return qs;
	}

	/**
	 * Matches a filter on the audit trail: {@code =id} for an exact id in any of
	 * the id columns, else a case-insensitive fragment of any of the id or name
	 * columns.
	 */
	private static void auditSearch(SelectQueryStruct qs, String value, List<String> ids, List<String> names) {
		value = text(value);
		if (value.isBlank()) {
			return;
		}
		OrQueryFilter any = new OrQueryFilter();
		if (value.startsWith("=")) {
			for (String id : ids) {
				any.addFilter(SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column(id), "==",
						value.substring(1)));
			}
		} else {
			String fragment = value.trim();
			for (String column : Stream.concat(ids.stream(), names.stream()).toList()) {
				any.addFilter(
						SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column(column), "?like", fragment));
			}
		}
		qs.addExplicitFilter(any);
	}

	/** Counts the rows the expression is not null on, or its distinct values. */
	private static IQuerySelector count(String alias, IQuerySelector counted, boolean distinct) {
		QueryFunctionSelector count = new QueryFunctionSelector();
		count.setFunction(QueryFunctionHelper.COUNT);
		count.setDistinct(distinct);
		count.addInnerSelector(counted);
		count.setAlias(alias);
		return count;
	}

	/** The events the audit source reads: the changes, and every denied request. */
	private static IQueryFilter auditEvents() {
		return new OrQueryFilter(eventTypeIn(CHANGE_EVENTS), denied());
	}

	private static IQueryFilter eventTypeIn(Set<AuditEventType> eventTypes) {
		return SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column("EVENT_TYPE"), "==",
				AuditReadQuerySupport.names(eventTypes));
	}

	private static IQueryFilter denied() {
		return SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column("STATUS"), "==",
				AuditStatus.DENIED.name());
	}

	/**
	 * A column of the events that meet the condition, else null, so counting it
	 * counts only those.
	 */
	private static IQuerySelector when(IQueryFilter condition, String column) {
		return QueryIfSelector.makeQueryIfSelector(condition,
				new QueryColumnSelector(AuditReadQuerySupport.column(column)), null, null);
	}

	private static Map<String, Object> auditSummaryRow(ResultSet rs) throws SQLException {
		Map<String, Object> row = new LinkedHashMap<>();
		for (String metric : AUDIT_METRICS) {
			row.put(metric, rs.getLong(metric));
		}
		return row;
	}

	/** Reads one event of the logs, its time as a UTC instant. */
	private static Map<String, Object> auditLogRow(ResultSet rs) throws SQLException {
		Map<String, Object> row = new LinkedHashMap<>();
		for (String column : AUDIT_LOG_COLUMNS) {
			row.put(column, column.equals("EVENT_TIME") ? AuditReadQuerySupport.utcTimestamp(rs.getTimestamp(column))
					: rs.getString(column));
		}
		return row;
	}

	private static Map<String, Object> auditDetailRow(ResultSet rs) throws SQLException {
		Map<String, Object> row = new LinkedHashMap<>();
		for (String column : AUDIT_DETAIL_COLUMNS) {
			row.put(column, rs.getString(column));
		}
		return row;
	}

	/**
	 * Lists registered identities for admin selectors without exposing unrelated
	 * account fields. Search and exact IDs are bound; pages include at most 100
	 * rows. User type is descriptive: inference logs identify users by ID, not
	 * provider.
	 */
	public static Map<String, Object> filterOptions(FilterDimension dimension, String searchTerm, String id, int limit,
			int offset) {
		ParameterizedQuery query = filterOptionsQuery(dimension, searchTerm, id, limit, offset);
		IRDBMSEngine engine = SystemEngineRegistry.getSecurityDb();
		if (engine == null) {
			throw new IllegalArgumentException("The Security Catalog Is Unavailable");
		}
		try {
			List<Map<String, Object>> rows = readRows(engine, query);
			boolean hasMore = rows.size() > limit;
			return Map.of("rows", hasMore ? new ArrayList<>(rows.subList(0, limit)) : rows, "hasMore", hasMore, "limit",
					limit, "offset", offset);
		} catch (Exception e) {
			classLogger.error("Unable to read enterprise filter options", e);
			throw new IllegalArgumentException("Unable To Load Filter Options");
		}
	}

	/**
	 * Creates an allowlisted catalog query with identity and engine provider
	 * metadata, fetching one extra row to identify the next page.
	 */
	static ParameterizedQuery filterOptionsQuery(FilterDimension dimension, String searchTerm, String id, int limit,
			int offset) {
		if (dimension == null || limit < 1 || limit > 100 || offset < 0 || offset > Integer.MAX_VALUE - 101) {
			throw new IllegalArgumentException("Invalid Filter Options Page");
		}
		String catalog = switch (dimension) {
		case USER ->
			"SELECT DISTINCT ID AS ENTITY_ID, COALESCE(NULLIF(NAME, ''), NULLIF(USERNAME, ''), ID) AS ENTITY_NAME, TYPE AS ENTITY_TYPE, '' AS ENTITY_SUBTYPE FROM SMSS_USER";
		case APP ->
			"SELECT DISTINCT PROJECTID AS ENTITY_ID, COALESCE(NULLIF(PROJECTDISPLAYNAME, ''), PROJECTNAME, PROJECTID) AS ENTITY_NAME, '' AS ENTITY_TYPE, '' AS ENTITY_SUBTYPE FROM PROJECT";
		case ENGINE ->
			"SELECT DISTINCT ENGINEID AS ENTITY_ID, COALESCE(NULLIF(ENGINEDISPLAYNAME, ''), ENGINENAME, ENGINEID) AS ENTITY_NAME, ENGINETYPE AS ENTITY_TYPE, ENGINESUBTYPE AS ENTITY_SUBTYPE FROM ENGINE";
		};
		List<Object> params = new ArrayList<>();
		StringBuilder where = new StringBuilder(" FROM (").append(catalog)
				.append(") entities WHERE ENTITY_ID IS NOT NULL");
		if (!text(id).isBlank()) {
			search(where, params, "ENTITY_ID", "ENTITY_NAME", "=" + id);
		} else {
			search(where, params, "ENTITY_ID", "ENTITY_NAME", searchTerm);
		}
		String sql = "SELECT ENTITY_ID, ENTITY_NAME, ENTITY_TYPE, ENTITY_SUBTYPE FROM (SELECT ENTITY_ID, ENTITY_NAME, ENTITY_TYPE, ENTITY_SUBTYPE, ROW_NUMBER() OVER (ORDER BY ENTITY_NAME, ENTITY_ID, ENTITY_TYPE) AS ROW_NUM"
				+ where + ") page_rows WHERE ROW_NUM > ? AND ROW_NUM <= ? ORDER BY ROW_NUM";
		params.add(offset);
		params.add(offset + limit + 1);
		return new ParameterizedQuery(sql, params, limit + 1);
	}

	/**
	 * Builds the read of a model message pair or an activity event's content as
	 * fixed SQL; an audit event's change is read with {@link #auditDetailQuery}.
	 */
	static ParameterizedQuery detailQuery(Source source, String recordId) {
		if (text(recordId).isBlank()) {
			throw new IllegalArgumentException("A recordId is required");
		}
		if (source == Source.AUDIT) {
			throw new IllegalArgumentException("The audit trail is read with its own query");
		}
		if (source == Source.ACTIVITY) {
			return new ParameterizedQuery("SELECT LOG_ID, MESSAGE, REQUEST, RESPONSE FROM AUDIT_LOGS WHERE LOG_ID = ?",
					List.of(recordId), 1);
		}
		// Derive the paired identity from the selected message on the server. The
		// client
		// cannot substitute a different user's transaction/model/room context.
		return new ParameterizedQuery("""
				SELECT m.MESSAGE_ID, m.MESSAGE_TYPE, m.MESSAGE_DATA, m.DATE_CREATED, m.MESSAGE_TOKENS, m.RESPONSE_TIME
				FROM MESSAGE m WHERE EXISTS (
				  SELECT 1 FROM MESSAGE selected WHERE selected.MESSAGE_ID = ?
				  AND COALESCE(m.TRANSACTION_ID, m.MESSAGE_ID) = COALESCE(selected.TRANSACTION_ID, selected.MESSAGE_ID)
				  AND (m.USER_ID = selected.USER_ID OR (m.USER_ID IS NULL AND selected.USER_ID IS NULL))
				  AND (m.AGENT_ID = selected.AGENT_ID OR (m.AGENT_ID IS NULL AND selected.AGENT_ID IS NULL))
				  AND (m.ROOM_ID = selected.ROOM_ID OR (m.ROOM_ID IS NULL AND selected.ROOM_ID IS NULL))
				) ORDER BY m.DATE_CREATED, m.MESSAGE_TYPE
				""", List.of(recordId), 20);
	}

	/**
	 * Retrieves retained bodies for a selected event, independently of aggregate
	 * reads.
	 */
	public static Map<String, Object> detail(Source source, String recordId) {
		if (source == Source.AUDIT) {
			return execute(source, 1, auditDetailQuery(recordId), EnterpriseUsageUtils::auditDetailRow);
		}
		return execute(source, detailQuery(source, recordId));
	}

	private static Map<String, Object> execute(Source source, ParameterizedQuery query) {
		return execute(source, query.limit(), engine -> readRows(engine, query));
	}

	private static Map<String, Object> execute(Source source, int limit, SelectQueryStruct qs,
			RowMapper<Map<String, Object>> mapper) {
		return execute(source, limit,
				engine -> QueryExecutionUtility.queryList(engine, qs, mapper, QUERY_TIMEOUT_SECONDS));
	}

	/** Reads a report's rows from the source's database. */
	@FunctionalInterface
	private interface RowsReader {
		List<Map<String, Object>> read(IRDBMSEngine engine) throws Exception;
	}

	private static Map<String, Object> execute(Source source, int limit, RowsReader reader) {
		IRDBMSEngine engine = engine(source);
		try {
			List<Map<String, Object>> rows = reader.read(engine);
			return Map.of("rows", rows, "limit", limit, "source", source.name().toLowerCase(Locale.ROOT));
		} catch (Exception e) {
			classLogger.error("Unable to read enterprise usage from {}", source, e);
			throw new IllegalArgumentException(
					"Unable to read enterprise usage. Verify the log database schema and availability.");
		}
	}

	/**
	 * The database a source reads from: the model inference logs, the activity
	 * logs, or the user tracking database that keeps the audit trail.
	 *
	 * @throws IllegalArgumentException when it is unavailable, or user tracking is
	 *                                  off for the audit source
	 */
	private static IRDBMSEngine engine(Source source) {
		IRDBMSEngine engine = switch (source) {
		case MODEL -> SystemEngineRegistry.getModelInferenceLogsDb();
		case ACTIVITY -> SystemEngineRegistry.getAuditLogsDb();
		case AUDIT -> Utility.isUserTrackingDisabled() || !SystemEngineRegistry.isUserTrackingDbLoaded() ? null
				: SystemEngineRegistry.getUserTrackingDb();
		};
		if (engine == null) {
			throw new IllegalArgumentException(
					"The requested log database is unavailable. Verify that logging is enabled.");
		}
		return engine;
	}

	/** Normalizes the enterprise API column/date contract across JDBC drivers. */
	private static List<Map<String, Object>> readRows(IRDBMSEngine engine, ParameterizedQuery query) {
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(engine, query, QUERY_TIMEOUT_SECONDS);
		// Keep the API's column names and date strings stable across JDBC drivers.
		rows.replaceAll(row -> {
			Map<String, Object> normalized = new LinkedHashMap<>();
			row.forEach((key, value) -> normalized.put(key.toUpperCase(Locale.ROOT),
					value instanceof SemossDate ? value.toString() : value));
			return normalized;
		});
		return rows;
	}

}
