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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Bounded admin reads; a committed append sequence fixes the paging snapshot.
 */
public final class AdminAuditReadService {

	private static final Map<String, String> FILTERS = Map.ofEntries(Map.entry("eventType", "EVENT_TYPE"),
			Map.entry("category", "CATEGORY"), Map.entry("status", "STATUS"), Map.entry("severity", "SEVERITY"),
			Map.entry("actorId", "ACTOR_USER_ID"), Map.entry("subjectId", "SUBJECT_USER_ID"),
			Map.entry("targetType", "TARGET_TYPE"), Map.entry("targetId", "TARGET_ID"),
			Map.entry("projectId", "PROJECT_ID"), Map.entry("engineId", "ENGINE_ID"),
			Map.entry("insightId", "INSIGHT_ID"));

	private AdminAuditReadService() {

	}

	public static Map<String, Object> read(User user, Map<String, String> options) throws Exception {
		AuditEventType eventType = "true".equals(options.get("export")) ? AuditEventType.AUDIT_EXPORT
				: AuditEventType.AUDIT_QUERY;
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(eventType).actorUser(user)
					.status(AuditStatus.DENIED).target(AuditTargetType.AUDIT, UserAuditTrailUtils.TABLE, null)
					.error(AuditErrorCode.ADMIN_REQUIRED, "Audit access requires an administrator"));
			throw new IllegalArgumentException("Audit access requires an administrator");
		}
		try {
			Map<String, Object> response = query(options);
			Map<String, Object> details = new LinkedHashMap<>();
			details.put("filters", options);
			details.put("snapshot", response.get("snapshot"));
			details.put("rowCount", response.get("rowCount"));
			details.put("phase", "SERVER_RETRIEVAL_COMPLETE");
			UserAuditTrailUtils.recordAuditAccess(user, eventType, details);
			return response;
		} catch (Exception failure) {
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(eventType).actorUser(user)
					.status(AuditStatus.FAILURE).target(AuditTargetType.AUDIT, UserAuditTrailUtils.TABLE, null)
					.error(AuditErrorCode.AUDIT_READ_FAILED, "Unable to retrieve audit events"));
			throw failure;
		}
	}

	private static Map<String, Object> query(Map<String, String> options) throws Exception {
		if (Utility.isUserTrackingDisabled() || !SystemEngineRegistry.isUserTrackingDbLoaded()) {
			throw new IllegalStateException("Audit tracking is disabled or its database is unavailable");
		}
		for (String key : options.keySet()) {
			if (!FILTERS.containsKey(key) && !Set.of("page", "snapshot", "export", "from", "to").contains(key)) {
				throw new IllegalArgumentException("Unsupported audit filter: " + key);
			}
		}
		String export = options.getOrDefault("export", "false");
		if (!Set.of("true", "false").contains(export)) {
			throw new IllegalArgumentException("Invalid export flag");
		}
		long page = number(options.getOrDefault("page", "0"));
		if (page > 1_000_000) {
			throw new IllegalArgumentException("Invalid audit page");
		}
		var engine = SystemEngineRegistry.getUserTrackingDb();
		long current = AuditChainStore.snapshot(engine);
		long snapshot = options.containsKey("snapshot") ? number(options.get("snapshot")) : current;
		if (snapshot > current) {
			throw new IllegalArgumentException("Invalid audit snapshot");
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String column : UserAuditTrailUtils.COLUMNS) {
			qs.addSelector(new QueryColumnSelector(AuditReadQuerySupport.column(column)));
		}
		// events appended after the snapshot are left out, so every page reads the same
		// events
		String sequence = AuditReadQuerySupport.column("AUDIT_SEQUENCE");
		qs.addExplicitFilter(
				new OrQueryFilter(SimpleQueryFilter.makeColToValFilter(sequence, "==", null, PixelDataType.NULL_VALUE),
						SimpleQueryFilter.makeColToValFilter(sequence, "<=", snapshot, PixelDataType.CONST_INT)));
		AuditReadQuerySupport.addExactFilters(qs, FILTERS, options, "Audit filter is too long");
		AuditReadQuerySupport.addDateFilters(qs, options, "Invalid audit date", "Invalid audit date range");
		qs.addOrderBy(new QueryColumnOrderBySelector(AuditReadQuerySupport.column("EVENT_TIME"), "DESC"));
		qs.addOrderBy(new QueryColumnOrderBySelector(AuditReadQuerySupport.column("EVENT_ID"), "DESC"));
		qs.setLimit(export.equals("true") ? 10000 : 26);
		qs.setOffSet(export.equals("true") ? 0 : page * 25);
		List<List<Object>> rows = QueryExecutionUtility.queryList(engine, qs, AdminAuditReadService::row,
				AuditReadQuerySupport.QUERY_TIMEOUT_SECONDS);
		// each event's engine or app as the catalog has it now, after the event's own
		// fields
		int typeIndex = UserAuditTrailUtils.COLUMNS.indexOf("TARGET_TYPE");
		int idIndex = UserAuditTrailUtils.COLUMNS.indexOf("TARGET_ID");
		AuditTargetCatalog catalog = AuditTargetCatalog.lookup(rows.stream()
				.map(row -> new String[] { (String) row.get(typeIndex), (String) row.get(idIndex) }).toList());
		for (List<Object> row : rows) {
			row.addAll(catalog.get((String) row.get(typeIndex), (String) row.get(idIndex)).values());
		}
		List<String> headers = new ArrayList<>(UserAuditTrailUtils.COLUMNS);
		headers.addAll(AuditTargetCatalog.COLUMNS);
		return Map.of("data", Map.of("headers", headers, "values", rows), "snapshot", Long.toString(snapshot),
				"rowCount", rows.size());
	}

	/**
	 * Reads one event in the order of {@link UserAuditTrailUtils#COLUMNS}, times as
	 * UTC instants
	 */
	private static List<Object> row(ResultSet rs) throws SQLException {
		List<Object> row = new ArrayList<>();
		for (String column : UserAuditTrailUtils.COLUMNS) {
			row.add(switch (column) {
			case "EVENT_TIME", "EVENT_OCCURRED_TIME" -> AuditReadQuerySupport.utcTimestamp(rs.getTimestamp(column));
			case "ACTOR_IS_ADMIN", "HTTP_STATUS" -> rs.getObject(column);
			default -> rs.getString(column);
			});
		}
		return row;
	}

	private static long number(String value) {
		try {
			if (value.matches("[0-9]{1,19}")) {
				return Long.parseLong(value);
			}
		} catch (NumberFormatException ignored) {
		}
		throw new IllegalArgumentException("Invalid audit pagination");
	}

}
