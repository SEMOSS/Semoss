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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Materialized, resource-scoped audit reads. Never exposes a query on
 * UserTrackingDatabase.
 */
public final class ResourceAuditReadService {

	private static final List<String> FIELDS = List.of("EVENT_ID", "EVENT_TIME", "EVENT_TYPE", "ACTION", "STATUS",
			"CATEGORY", "SEVERITY", "ACTOR_USER_ID", "ACTOR_USER_TYPE", "ACTOR_USER_NAME", "SUBJECT_USER_ID",
			"SUBJECT_USER_TYPE", "SUBJECT_USER_NAME", "TARGET_TYPE", "TARGET_ID", "TARGET_NAME", "OLD_VALUE",
			"NEW_VALUE", "DETAILS");
	private static final Map<String, String> FILTERS = Map.of("eventType", "EVENT_TYPE", "category", "CATEGORY",
			"status", "STATUS", "severity", "SEVERITY", "actorId", "ACTOR_USER_ID", "subjectId", "SUBJECT_USER_ID");
	private static final Set<String> SORTS = Set.of("EVENT_TIME", "EVENT_TYPE", "STATUS", "SEVERITY",
			"ACTOR_USER_NAME");
	/** The access changes on a project or an engine its owners see */
	private static final Set<AuditEventType> COMMON_EVENTS = EnumSet.of(AuditEventType.PERMISSION_ADD,
			AuditEventType.PERMISSION_UPDATE, AuditEventType.PERMISSION_DELETE, AuditEventType.PERMISSION_BULK_GRANT,
			AuditEventType.ACCESS_REQUEST_APPROVE, AuditEventType.ACCESS_REQUEST_REJECT);
	private static final Set<AuditEventType> PROJECT_EVENTS = EnumSet.of(AuditEventType.PROJECT_CREATE,
			AuditEventType.PROJECT_UPDATE, AuditEventType.PROJECT_DELETE, AuditEventType.PROJECT_UPLOAD,
			AuditEventType.WORKSPACE_CREATE, AuditEventType.WORKSPACE_UPDATE, AuditEventType.WORKSPACE_DELETE,
			AuditEventType.SKILL_CREATE, AuditEventType.SKILL_UPDATE, AuditEventType.SKILL_DELETE,
			AuditEventType.AUTOMATION_CREATE, AuditEventType.AUTOMATION_UPDATE);
	/**
	 * A usage limit set, changed or removed on a model; its owners set them, so
	 * they see the limits
	 */
	private static final AuditEventType USAGE_LIMIT_EVENT = AuditEventType.USAGE_LIMIT_UPDATE;
	private static final Set<AuditEventType> ENGINE_EVENTS = EnumSet.of(AuditEventType.ENGINE_CREATE,
			AuditEventType.ENGINE_UPDATE, AuditEventType.ENGINE_DELETE, AuditEventType.MODEL_CREATE,
			AuditEventType.VECTOR_CREATE, AuditEventType.STORAGE_CREATE, AuditEventType.FUNCTION_CREATE,
			AuditEventType.GUARDRAIL_CREATE, USAGE_LIMIT_EVENT);
	/**
	 * The targets that are the project itself, or the engine itself, rather than
	 * something in it
	 */
	private static final Set<AuditTargetType> PROJECT_TARGETS = EnumSet.of(AuditTargetType.PROJECT,
			AuditTargetType.WORKSPACE, AuditTargetType.SKILL, AuditTargetType.AUTOMATION);
	private static final Set<AuditTargetType> ENGINE_TARGETS = EnumSet.of(AuditTargetType.ENGINE,
			AuditTargetType.DATABASE, AuditTargetType.MODEL, AuditTargetType.VECTOR, AuditTargetType.STORAGE,
			AuditTargetType.FUNCTION, AuditTargetType.GUARDRAIL, AuditTargetType.VENV);
	/** The fields of a usage limit rule an owner sees */
	private static final List<String> RULE_FIELDS = List.of("metric", "amount", "window", "windowLength", "counts");
	/** The alias of the severity's rank, which a sort by severity orders by */
	private static final String SEVERITY_RANK = "SEVERITY_RANK";

	private ResourceAuditReadService() {
	}

	public static Map<String, Object> read(User user, String scope, String id, Map<String, String> options)
			throws Exception {
		if (!Set.of("ENGINE", "PROJECT").contains(scope) || id == null || id.isBlank() || id.length() > 255) {
			throw new IllegalArgumentException("A valid engine or project ID is required.");
		}
		if (!isOwner(user, scope, id)) {
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(AuditEventType.AUDIT_QUERY)
					.status(AuditStatus.DENIED).actorUser(user).target(AuditTargetType.valueOf(scope), id, null)
					.context(scope.equals("PROJECT") ? id : null, scope.equals("ENGINE") ? id : null, null, null)
					.error(AuditErrorCode.AUDIT_ACCESS_DENIED, "Resource audit access denied."));
			throw new IllegalArgumentException(
					"Only administrators and current resource owners can view this audit trail.");
		}
		if (Utility.isUserTrackingDisabled() || !SystemEngineRegistry.isUserTrackingDbLoaded()) {
			throw new IllegalStateException("Audit tracking is disabled or its database is unavailable.");
		}
		for (String key : options.keySet()) {
			if (!FILTERS.containsKey(key)
					&& !Set.of("page", "pageSize", "sort", "direction", "from", "to").contains(key)) {
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
		Set<AuditEventType> events = EnumSet.copyOf(COMMON_EVENTS);
		events.addAll(scope.equals("PROJECT") ? PROJECT_EVENTS : ENGINE_EVENTS);
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String field : FIELDS) {
			qs.addSelector(new QueryColumnSelector(AuditReadQuerySupport.column(field)));
		}
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column(scope + "_ID"), "==", id));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column("TARGET_ID"), "==", id));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column("TARGET_TYPE"), "==",
				AuditReadQuerySupport.names(scope.equals("PROJECT") ? PROJECT_TARGETS : ENGINE_TARGETS)));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AuditReadQuerySupport.column("EVENT_TYPE"), "==",
				AuditReadQuerySupport.names(events)));
		AuditReadQuerySupport.addExactFilters(qs, FILTERS, options, "Audit filter is too long.");
		AuditReadQuerySupport.addDateFilters(qs, options, "Invalid audit date. Use YYYY-MM-DD.",
				"Invalid audit date range.");
		if (sort.equals("SEVERITY")) {
			qs.addSelector(severityRank());
			qs.addOrderBy(new QueryColumnOrderBySelector(SEVERITY_RANK, direction));
		} else {
			qs.addOrderBy(new QueryColumnOrderBySelector(AuditReadQuerySupport.column(sort), direction));
		}
		if (!sort.equals("EVENT_TIME")) {
			qs.addOrderBy(new QueryColumnOrderBySelector(AuditReadQuerySupport.column("EVENT_TIME"), "DESC"));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector(AuditReadQuerySupport.column("EVENT_ID"), direction));
		qs.setLimit(pageSize + 1L);
		qs.setOffSet((long) page * pageSize);
		List<Map<String, Object>> rows = QueryExecutionUtility.queryList(SystemEngineRegistry.getUserTrackingDb(), qs,
				ResourceAuditReadService::row, AuditReadQuerySupport.QUERY_TIMEOUT_SECONDS);
		boolean hasNext = rows.size() > pageSize;
		if (hasNext) {
			rows.remove(rows.size() - 1);
		}
		AuditTargetCatalog.addTo(rows);
		UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(AuditEventType.AUDIT_QUERY)
				.status(AuditStatus.SUCCESS).actorUser(user).target(AuditTargetType.valueOf(scope), id, null)
				.context(scope.equals("PROJECT") ? id : null, scope.equals("ENGINE") ? id : null, null, null)
				.details(Map.of("page", page, "pageSize", pageSize, "ownerView", true)));
		return Map.of("events", rows, "hasNextPage", hasNext, "page", page, "pageSize", pageSize);
	}

	/**
	 * Whether the user can read a resource's audit trail: an admin, or an owner of
	 * the project or engine now, directly or through a team. A deleted resource's
	 * audit trail is for admins only, whatever grants remain.
	 */
	static boolean isOwner(User user, String scope, String id) throws Exception {
		if (user == null || user.isAnonymous()) {
			return false;
		}
		if (SecurityAdminUtils.userIsAdmin(user)) {
			return true;
		}
		if (!resourceExists(scope, id)) {
			return false;
		}
		return scope.equals("ENGINE") ? SecurityEngineUtils.userIsOwner(user, id)
				: SecurityProjectUtils.userIsOwner(user, id);
	}

	private static boolean resourceExists(String scope, String id) throws Exception {
		String column = scope + "__" + scope + "ID";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(column));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column, "==", id));
		qs.setLimit(1);
		return !QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(), qs, result -> Boolean.TRUE)
				.isEmpty();
	}

	/**
	 * Ranks the severity by risk, in the order {@link AuditSeverity} declares it,
	 * {@code LOW} 1 to {@code CRITICAL} 4 and anything else 0, so a sort by
	 * severity is not alphabetical.
	 */
	private static IQuerySelector severityRank() {
		IQuerySelector rank = rank(0);
		AuditSeverity[] severities = AuditSeverity.values();
		for (int i = severities.length - 1; i >= 0; i--) {
			rank = QueryIfSelector.makeQueryIfSelector(SimpleQueryFilter.makeColToValFilter(
					AuditReadQuerySupport.column("SEVERITY"), "==", severities[i].name()), rank(i + 1), rank, null);
		}
		((QueryIfSelector) rank).setAlias(SEVERITY_RANK);
		return rank;
	}

	/** A rank, typed so that every database can read the bound value */
	private static IQuerySelector rank(int value) {
		QueryFunctionSelector cast = new QueryFunctionSelector();
		cast.setFunction(QueryFunctionHelper.CAST);
		cast.setDataType("INTEGER");
		cast.addInnerSelector(new QueryConstantSelector(value));
		return cast;
	}

	/** Reads one event, keeping only what an owner may see of its change */
	private static Map<String, Object> row(ResultSet rs) throws SQLException {
		AuditEventType eventType = AuditEventType.find(rs.getString("EVENT_TYPE"));
		Map<String, Object> row = new LinkedHashMap<>();
		for (String field : FIELDS) {
			String value = rs.getString(field);
			switch (field) {
			case "EVENT_TIME" -> value = AuditReadQuerySupport.utcTimestamp(rs.getTimestamp(field));
			// Change payloads can contain private metadata; only permission transitions and
			// a
			// model's usage limits are owner-visible.
			case "OLD_VALUE", "NEW_VALUE" ->
				value = eventType == USAGE_LIMIT_EVENT ? usageLimitValue(value) : permissionValue(value);
			case "DETAILS" -> value = permissionDetails(value, eventType);
			default -> {
			}
			}
			row.put(field, value);
		}
		return row;
	}

	private static String permissionValue(String value) {
		if (value == null) {
			return null;
		}
		try {
			var object = JsonParser.parseString(value).getAsJsonObject();
			String permission = object.has("permission") ? object.get("permission").getAsString() : "";
			return Set.of("OWNER", "EDIT", "READ_ONLY").contains(permission) ? permission : null;
		} catch (RuntimeException ignored) {
			return null;
		}
	}

	/**
	 * Keeps only the limit of a usage limit change's old or new value,
	 * {@code {"usageLimit": {"unlimited", "rules": [...]}}}, with each rule's
	 * measure and amount; null for no limit defined.
	 */
	private static String usageLimitValue(String value) {
		if (value == null) {
			return null;
		}
		try {
			var object = JsonParser.parseString(value).getAsJsonObject();
			var safe = new JsonObject();
			var limit = object.get("usageLimit");
			if (limit == null || !limit.isJsonObject()) {
				safe.add("usageLimit", JsonNull.INSTANCE);
				return safe.toString();
			}
			var safeLimit = new JsonObject();
			var unlimited = limit.getAsJsonObject().get("unlimited");
			safeLimit.addProperty("unlimited",
					unlimited != null && unlimited.isJsonPrimitive() && unlimited.getAsBoolean());
			var rules = new JsonArray();
			var sentRules = limit.getAsJsonObject().get("rules");
			if (sentRules != null && sentRules.isJsonArray()) {
				for (var rule : sentRules.getAsJsonArray()) {
					if (!rule.isJsonObject()) {
						continue;
					}
					var safeRule = new JsonObject();
					for (String key : RULE_FIELDS) {
						var item = rule.getAsJsonObject().get(key);
						if (item != null && item.isJsonPrimitive()) {
							safeRule.add(key, item);
						}
					}
					rules.add(safeRule);
				}
			}
			safeLimit.add("rules", rules);
			safe.add("usageLimit", safeLimit);
			return safe.toString();
		} catch (RuntimeException ignored) {
			return null;
		}
	}

	private static String permissionDetails(String value, AuditEventType eventType) {
		if (value != null && eventType == USAGE_LIMIT_EVENT) {
			return usageLimitDetails(value);
		}
		if (value == null || !COMMON_EVENTS.contains(eventType)) {
			return null;
		}
		try {
			var object = JsonParser.parseString(value).getAsJsonObject();
			var safe = new JsonObject();
			for (String key : List.of("groupId", "groupType", "granteeId", "granteeType")) {
				var item = object.get(key);
				if (item != null && item.isJsonPrimitive() && item.getAsJsonPrimitive().isString()
						&& item.getAsString().length() <= 255) {
					safe.addProperty(key, item.getAsString());
				}
			}
			if (object.has("reason") && object.get("reason").isJsonPrimitive()
					&& "EXPIRED".equals(object.get("reason").getAsString())) {
				safe.addProperty("reason", "EXPIRED");
			}
			return safe.size() == 0 ? null : safe.toString();
		} catch (RuntimeException ignored) {
			return null;
		}
	}

	/**
	 * Keeps which limit changed, the team for a team's limit, and whether it went
	 * with its grant.
	 */
	private static String usageLimitDetails(String value) {
		try {
			var object = JsonParser.parseString(value).getAsJsonObject();
			var safe = new JsonObject();
			for (String key : List.of("limitType", "groupId", "groupType")) {
				var item = object.get(key);
				if (item != null && item.isJsonPrimitive() && item.getAsJsonPrimitive().isString()
						&& item.getAsString().length() <= 255) {
					safe.addProperty(key, item.getAsString());
				}
			}
			var removed = object.get("removedWithGrant");
			if (removed != null && removed.isJsonPrimitive() && removed.getAsJsonPrimitive().isBoolean()) {
				safe.addProperty("removedWithGrant", removed.getAsBoolean());
			}
			if (object.has("reason") && object.get("reason").isJsonPrimitive()
					&& "EXPIRED".equals(object.get("reason").getAsString())) {
				safe.addProperty("reason", "EXPIRED");
			}
			return safe.size() == 0 ? null : safe.toString();
		} catch (RuntimeException ignored) {
			return null;
		}
	}

	private static int integer(String value, int min, int max) {
		try {
			int number = Integer.parseInt(value);
			if (number >= min && number <= max) {
				return number;
			}
		} catch (NumberFormatException ignored) {
		}
		throw new IllegalArgumentException("Invalid audit pagination.");
	}

}
