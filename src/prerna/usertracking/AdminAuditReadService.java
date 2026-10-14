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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/** Bounded admin reads; a committed append sequence fixes the paging snapshot. */
public final class AdminAuditReadService {
    private static final Map<String, String> FILTERS = Map.ofEntries(
            Map.entry("eventType", "EVENT_TYPE"), Map.entry("category", "CATEGORY"), Map.entry("status", "STATUS"),
            Map.entry("severity", "SEVERITY"), Map.entry("actorId", "ACTOR_USER_ID"), Map.entry("subjectId", "SUBJECT_USER_ID"),
            Map.entry("targetType", "TARGET_TYPE"), Map.entry("targetId", "TARGET_ID"), Map.entry("projectId", "PROJECT_ID"),
            Map.entry("engineId", "ENGINE_ID"), Map.entry("insightId", "INSIGHT_ID"));

    private AdminAuditReadService() { }

    public static Map<String, Object> read(User user, Map<String, String> options) throws Exception {
        String eventType = "true".equals(options.get("export")) ? "AUDIT_EXPORT" : "AUDIT_QUERY";
        if (!SecurityAdminUtils.userIsAdmin(user)) {
            UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(eventType).actorUser(user)
                    .status("DENIED").target("AUDIT", UserAuditTrailUtils.TABLE, null)
                    .error("ADMIN_REQUIRED", "Audit access requires an administrator"));
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
                    .status("FAILURE").target("AUDIT", UserAuditTrailUtils.TABLE, null)
                    .error("AUDIT_READ_FAILED", "Unable to retrieve audit events"));
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
        if (!Set.of("true", "false").contains(export)) throw new IllegalArgumentException("Invalid export flag");
        long page = number(options.getOrDefault("page", "0"));
        if (page > 1_000_000) throw new IllegalArgumentException("Invalid audit page");
        var engine = SystemEngineRegistry.getUserTrackingDb();
        long current = AuditChainStore.snapshot(engine);
        long snapshot = options.containsKey("snapshot") ? number(options.get("snapshot")) : current;
        if (snapshot > current) throw new IllegalArgumentException("Invalid audit snapshot");
        StringBuilder sql = new StringBuilder("SELECT " + String.join(",", UserAuditTrailUtils.COLUMNS)
                + " FROM USER_AUDIT_EVENTS WHERE (AUDIT_SEQUENCE IS NULL OR AUDIT_SEQUENCE<=?)");
        List<Object> parameters = new ArrayList<>();
        parameters.add(snapshot);
        AuditReadQuerySupport.appendExactFilters(sql, parameters, FILTERS, options, "Audit filter is too long");
        AuditReadQuerySupport.appendDateFilters(sql, parameters, options, "Invalid audit date", "Invalid audit date range");
        sql.append(" ORDER BY EVENT_TIME DESC,EVENT_ID DESC");
        int limit = export.equals("true") ? 10000 : 26;
        String query = engine.getQueryUtil().addLimitOffsetToQuery(sql, limit, export.equals("true") ? 0 : page * 25).toString();
        List<List<Object>> rows = QueryExecutionUtility.read(engine, connection -> {
            List<List<Object>> values = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                AuditReadQuerySupport.bind(ps, parameters);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();
                        for (String column : UserAuditTrailUtils.COLUMNS) {
                            Object value = switch (column) {
                                case "EVENT_TIME", "EVENT_OCCURRED_TIME" -> AuditReadQuerySupport.utcTimestamp(rs.getTimestamp(column));
                                case "ACTOR_IS_ADMIN", "HTTP_STATUS" -> rs.getObject(column);
                                default -> rs.getString(column);
                            };
                            row.add(value);
                        }
                        values.add(row);
                    }
                }
            }
            return values;
        });
        return Map.of("data", Map.of("headers", UserAuditTrailUtils.COLUMNS, "values", rows),
                "snapshot", Long.toString(snapshot), "rowCount", rows.size());
    }

    private static long number(String value) {
        try {
            if (value.matches("[0-9]{1,19}")) return Long.parseLong(value);
        } catch (NumberFormatException ignored) { }
        throw new IllegalArgumentException("Invalid audit pagination");
    }

}
