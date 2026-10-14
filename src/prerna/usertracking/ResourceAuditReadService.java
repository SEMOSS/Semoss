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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonParser;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/** Materialized, resource-scoped audit reads. Never exposes a query on UserTrackingDatabase. */
public final class ResourceAuditReadService {
    private static final List<String> FIELDS = List.of("EVENT_ID", "EVENT_TIME", "EVENT_TYPE", "ACTION",
            "STATUS", "CATEGORY", "SEVERITY", "ACTOR_USER_ID", "ACTOR_USER_TYPE", "ACTOR_USER_NAME",
            "SUBJECT_USER_ID", "SUBJECT_USER_TYPE", "SUBJECT_USER_NAME", "TARGET_TYPE", "TARGET_ID",
            "TARGET_NAME", "OLD_VALUE", "NEW_VALUE", "DETAILS");
    private static final Map<String, String> FILTERS = Map.of("eventType", "EVENT_TYPE", "category", "CATEGORY",
            "status", "STATUS", "severity", "SEVERITY", "actorId", "ACTOR_USER_ID", "subjectId", "SUBJECT_USER_ID");
    private static final Set<String> SORTS = Set.of("EVENT_TIME", "EVENT_TYPE", "STATUS", "SEVERITY", "ACTOR_USER_NAME");
    private static final List<String> COMMON_EVENTS = List.of("PERMISSION_ADD", "PERMISSION_UPDATE",
            "PERMISSION_DELETE", "PERMISSION_EXPIRE", "PERMISSION_BULK_GRANT", "ACCESS_REQUEST_APPROVE",
            "ACCESS_REQUEST_REJECT");

    private ResourceAuditReadService() { }

    public static Map<String, Object> read(User user, String scope, String id, Map<String, String> options) throws Exception {
        if (!Set.of("ENGINE", "PROJECT").contains(scope) || id == null || id.isBlank() || id.length() > 255) {
            throw new IllegalArgumentException("A valid engine or project ID is required.");
        }
        if (!isOwner(user, scope, id)) {
            UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType("AUDIT_QUERY")
                    .action("AUDIT_QUERY").status("DENIED").actorUser(user).target(scope, id, null)
                    .context(scope.equals("PROJECT") ? id : null, scope.equals("ENGINE") ? id : null, null, null)
                    .error("AUDIT_ACCESS_DENIED", "Resource audit access denied."));
            throw new IllegalArgumentException("Only administrators and current resource owners can view this audit trail.");
        }
        if (Utility.isUserTrackingDisabled() || !SystemEngineRegistry.isUserTrackingDbLoaded()) {
            throw new IllegalStateException("Audit tracking is disabled or its database is unavailable.");
        }
        for (String key : options.keySet()) {
            if (!FILTERS.containsKey(key) && !Set.of("page", "pageSize", "sort", "direction", "from", "to").contains(key)) {
                throw new IllegalArgumentException("Unsupported audit filter: " + key);
            }
        }
        int page = integer(options.getOrDefault("page", "0"), 0, 1000);
        int pageSize = integer(options.getOrDefault("pageSize", "25"), 1, 100);
        String sort = options.getOrDefault("sort", "EVENT_TIME");
        String direction = options.getOrDefault("direction", "DESC");
        if (!SORTS.contains(sort) || !Set.of("ASC", "DESC").contains(direction)) {
            throw new IllegalArgumentException("Unsupported audit sort.");
        }
        List<String> events = new ArrayList<>(COMMON_EVENTS);
        if (scope.equals("PROJECT")) {
            events.addAll(List.of("PROJECT_CREATE", "PROJECT_UPDATE", "PROJECT_DELETE", "PROJECT_UPLOAD",
                    "WORKSPACE_CREATE", "WORKSPACE_UPDATE", "WORKSPACE_DELETE", "SKILL_CREATE", "SKILL_UPDATE",
                    "SKILL_DELETE", "AUTOMATION_CREATE", "AUTOMATION_UPDATE", "AUTOMATION_DELETE"));
        } else {
            events.addAll(List.of("ENGINE_CREATE", "ENGINE_UPDATE", "ENGINE_DELETE", "ENGINE_UPLOAD",
                    "MODEL_CREATE", "VECTOR_CREATE", "STORAGE_CREATE", "FUNCTION_CREATE", "GUARDRAIL_CREATE", "VENV_CREATE"));
        }
        String types = scope.equals("PROJECT") ? "'PROJECT','WORKSPACE','SKILL','AUTOMATION'" : "'ENGINE','DATABASE','MODEL','VECTOR','STORAGE','FUNCTION','GUARDRAIL','VENV'";
        StringBuilder sql = new StringBuilder("SELECT " + String.join(",", FIELDS) + " FROM " + UserAuditTrailUtils.TABLE
                + " WHERE " + scope + "_ID=? AND TARGET_ID=? AND TARGET_TYPE IN (" + types + ") AND EVENT_TYPE IN ("
                + String.join(",", java.util.Collections.nCopies(events.size(), "?")) + ")");
        List<Object> parameters = new ArrayList<>(List.of(id, id));
        parameters.addAll(events);
        AuditReadQuerySupport.appendExactFilters(sql, parameters, FILTERS, options, "Audit filter is too long.");
        AuditReadQuerySupport.appendDateFilters(sql, parameters, options,
                "Invalid audit date. Use YYYY-MM-DD.", "Invalid audit date range.");
        String sortExpression = sort.equals("SEVERITY")
                ? "CASE SEVERITY WHEN 'LOW' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'HIGH' THEN 3 WHEN 'CRITICAL' THEN 4 ELSE 0 END"
                : sort;
        sql.append(" ORDER BY ").append(sortExpression).append(" ").append(direction);
        if (!sort.equals("EVENT_TIME")) sql.append(",EVENT_TIME DESC");
        sql.append(",EVENT_ID ").append(direction);
        IRDBMSEngine engine = SystemEngineRegistry.getUserTrackingDb();
        String query = engine.getQueryUtil().addLimitOffsetToQuery(sql, pageSize + 1L, (long) page * pageSize).toString();
        List<Map<String, Object>> rows = QueryExecutionUtility.read(engine, connection -> {
            List<Map<String, Object>> result = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                AuditReadQuerySupport.bind(statement, parameters);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (String field : FIELDS) {
                            String value = rs.getString(field);
                            if (field.equals("EVENT_TIME") && value != null) value = AuditReadQuerySupport.utcTimestamp(rs.getTimestamp(field));
                            // Change payloads can contain private metadata; only permission transitions are owner-visible.
                            if (field.equals("OLD_VALUE") || field.equals("NEW_VALUE")) value = permissionValue(value);
                            if (field.equals("DETAILS")) value = permissionDetails(value, rs.getString("EVENT_TYPE"));
                            row.put(field, value);
                        }
                        result.add(row);
                    }
                }
            }
            return result;
        });
        boolean hasNext = rows.size() > pageSize;
        if (hasNext) rows.remove(rows.size() - 1);
        UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType("AUDIT_QUERY").action("AUDIT_QUERY")
                .status("SUCCESS").actorUser(user).target(scope, id, null)
                .context(scope.equals("PROJECT") ? id : null, scope.equals("ENGINE") ? id : null, null, null)
                .details(Map.of("page", page, "pageSize", pageSize, "ownerView", true)));
        return Map.of("events", rows, "hasNextPage", hasNext, "page", page, "pageSize", pageSize);
    }

    static boolean isOwner(User user, String scope, String id) throws Exception {
        if (user == null || user.isAnonymous()) return false;
        if (SecurityAdminUtils.userIsAdmin(user)) return true;
        if (!resourceExists(scope, id)) return false;
        for (var login : user.getLogins()) {
            AccessToken token = user.getAccessToken(login);
            if (hasGrant(scope + "PERMISSION", scope + "ID", id, "USERID", token.getId(), null)) return true;
            if (hasGroupGrant(scope, id, token.getUserGroupType(), token.getUserGroups())) return true;
            if (hasGroupGrant(scope, id, "CUSTOM", AdminSecurityGroupUtils.getUserCustomGroups(token))) return true;
        }
        return false;
    }

    private static boolean resourceExists(String scope, String id) throws Exception {
        return QueryExecutionUtility.read(SystemEngineRegistry.getSecurityDb(), connection -> {
            try (PreparedStatement statement = connection.prepareStatement("SELECT " + scope + "ID FROM " + scope + " WHERE " + scope + "ID=?")) {
                statement.setString(1, id);
                try (ResultSet result = statement.executeQuery()) { return result.next(); }
            }
        });
    }

    private static boolean hasGroupGrant(String scope, String id, String type, Collection<String> groups) throws Exception {
        if (type == null || groups == null) return false;
        for (String group : groups) {
            if (hasGrant("GROUP" + scope + "PERMISSION", scope + "ID", id, "ID", group, type)) return true;
        }
        return false;
    }

    private static boolean hasGrant(String table, String resourceColumn, String id, String principalColumn,
            String principal, String type) throws Exception {
        String sql = "SELECT PERMISSION FROM " + table + " WHERE " + resourceColumn + "=? AND " + principalColumn
                + "=? AND PERMISSION=1 AND (ENDDATE IS NULL OR ENDDATE>?)" + (type == null ? "" : " AND TYPE=?");
        return QueryExecutionUtility.read(SystemEngineRegistry.getSecurityDb(), connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, id);
                statement.setString(2, principal);
                statement.setTimestamp(3, Timestamp.from(java.time.Instant.now()));
                if (type != null) statement.setString(4, type);
                try (ResultSet result = statement.executeQuery()) { return result.next(); }
            }
        });
    }

    private static String permissionValue(String value) {
        if (value == null) return null;
        try {
            var object = JsonParser.parseString(value).getAsJsonObject();
            String permission = object.has("permission") ? object.get("permission").getAsString() : "";
            return Set.of("OWNER", "EDIT", "READ_ONLY").contains(permission) ? permission : null;
        } catch (RuntimeException ignored) { return null; }
    }

    private static String permissionDetails(String value, String eventType) {
        if (value == null || !COMMON_EVENTS.contains(eventType)) return null;
        try {
            var object = JsonParser.parseString(value).getAsJsonObject();
            var safe = new com.google.gson.JsonObject();
            for (String key : List.of("groupId", "groupType", "granteeId", "granteeType")) {
                var item = object.get(key);
                if (item != null && item.isJsonPrimitive() && item.getAsJsonPrimitive().isString()
                        && item.getAsString().length() <= 255) safe.addProperty(key, item.getAsString());
            }
            if (object.has("reason") && object.get("reason").isJsonPrimitive()
                    && "EXPIRED".equals(object.get("reason").getAsString())) safe.addProperty("reason", "EXPIRED");
            return safe.size() == 0 ? null : safe.toString();
        } catch (RuntimeException ignored) { return null; }
    }

    private static int integer(String value, int min, int max) {
        try {
            int number = Integer.parseInt(value);
            if (number >= min && number <= max) return number;
        } catch (NumberFormatException ignored) { }
        throw new IllegalArgumentException("Invalid audit pagination.");
    }

}
