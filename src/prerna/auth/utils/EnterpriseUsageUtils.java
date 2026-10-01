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

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;
import prerna.util.SystemEngineRegistry;

/** Fixed, parameterized analytics over the two internal logging databases. */
public final class EnterpriseUsageUtils {

	private static final Logger classLogger = LogManager.getLogger(EnterpriseUsageUtils.class);
	public static final int MAX_LIMIT = 5000;

	public enum Source {
		MODEL, ACTIVITY
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

	private static String scope(Source source, String from, String to, String user, String app, String engineFilter,
			List<Object> params) {
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
		String time = source == Source.MODEL ? "m.DATE_CREATED" : "l.LOG_TIMESTAMP";
		StringBuilder sql = new StringBuilder(source == Source.MODEL ? MODEL_FROM : "FROM AUDIT_LOGS l");
		sql.append(" WHERE ").append(time).append(" >= ? AND ").append(time).append(" < ?");
		params.add(Timestamp.valueOf(start.atStartOfDay()));
		params.add(Timestamp.valueOf(end.plusDays(1).atStartOfDay()));
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

	static ParameterizedQuery reportQuery(Source source, View view, String from, String to, String user, String app,
			String engineFilter, String dimension, int limit, int offset) {
		if (limit < 1 || limit > MAX_LIMIT || offset < 0 || offset > Integer.MAX_VALUE - MAX_LIMIT) {
			throw new IllegalArgumentException("Invalid pagination value");
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
		return execute(source, reportQuery(source, view, from, to, user, app, engineFilter, dimension, limit, offset));
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

	static ParameterizedQuery detailQuery(Source source, String recordId) {
		if (text(recordId).isBlank()) {
			throw new IllegalArgumentException("A recordId is required");
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
		return execute(source, detailQuery(source, recordId));
	}

	private static Map<String, Object> execute(Source source, ParameterizedQuery query) {
		IRDBMSEngine engine = source == Source.MODEL ? SystemEngineRegistry.getModelInferenceLogsDb()
				: SystemEngineRegistry.getAuditLogsDb();
		if (engine == null) {
			throw new IllegalArgumentException(
					"The requested log database is unavailable. Verify that logging is enabled.");
		}
		try {
			List<Map<String, Object>> rows = readRows(engine, query);
			return Map.of("rows", rows, "limit", query.limit(), "source", source.name().toLowerCase(Locale.ROOT));
		} catch (Exception e) {
			classLogger.error("Unable to read enterprise usage from {}", source, e);
			throw new IllegalArgumentException(
					"Unable to read enterprise usage. Verify the log database schema and availability.");
		}
	}

	/** Normalizes the enterprise API column/date contract across JDBC drivers. */
	private static List<Map<String, Object>> readRows(IRDBMSEngine engine, ParameterizedQuery query) {
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(engine, query, 30);
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
