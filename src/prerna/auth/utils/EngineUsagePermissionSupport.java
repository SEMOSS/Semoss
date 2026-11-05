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

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.javatuples.Pair;
import prerna.auth.User;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionSubject.SubjectCategory;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.usage.UsageLimit;
import prerna.engine.impl.usage.UsageLimitKey;
import prerna.engine.impl.usage.UsageLimitKey.Source;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.ValueUtils;

/**
 * New catalog policies participate in the existing permission-batch
 * transaction.
 */
final class EngineUsagePermissionSupport {
	private EngineUsagePermissionSupport() {
	}

	static Context read(String engineId, boolean isModel) {
		if (isModel)
			return null;
		try {
			if (SecurityEngineUtils.getEngineType(engineId) != CATALOG_TYPE.DATABASE)
				return null;
		} catch (IllegalArgumentException missing) {
			return null;
		}
		return new Context(engineId, SecurityEngineUsageLimitUtils.readEnginePolicies(CATALOG_TYPE.DATABASE, engineId));
	}

	static final class Context {
		private final String engineId;
		private final Map<UsageLimitKey, UsageLimit> policies = new LinkedHashMap<>();

		Context(String engineId, List<SecurityEngineUsageLimitUtils.Entry> entries) {
			this.engineId = engineId;
			for (var entry : entries)
				policies.put(entry.key(), entry.usageLimit());
		}

		UsageLimit current(Source source, String id, String type) {
			if (source != Source.DIRECT || type != null)
				return policies.get(key(source, id, type));
			UsageLimit only = null;
			for (var entry : policies.entrySet())
				if (entry.getKey().source() == Source.DIRECT && id.equals(entry.getKey().subjectId())) {
					if (only != null)
						return null;
					only = entry.getValue();
				}
			return only;
		}

		boolean hasDirect(String id) {
			return policies.keySet().stream().anyMatch(k -> k.source() == Source.DIRECT && id.equals(k.subjectId()));
		}

		UsageLimitKey key(Source source, String id, String type) {
			return new UsageLimitKey(CATALOG_TYPE.DATABASE, source, engineId, id, type);
		}

		void decorateGrant(Map<String, Object> grant, Source source, String id, String type) {
			grant.put("engineUsageLimit", current(source, id, type));
			grant.put("engineUsageCatalog", CATALOG_TYPE.DATABASE.name());
		}

		void decorateSettings(Map<String, Object> result, Map<String, Object> allowed, boolean owner) {
			allowed.put("canChangeEngineUsageLimits", owner);
			Map<String, Object> settings = new LinkedHashMap<>();
			settings.put("catalogType", CATALOG_TYPE.DATABASE.name());
			settings.put("metrics", List.of("QUERIES", "ROWS", "COMPUTE_TIME"));
			Map<String, Object> publicUse = new LinkedHashMap<>();
			publicUse.put("isPublic", SecurityEngineUtils.engineIsGlobal(engineId));
			publicUse.put("usageLimit", current(Source.PUBLIC, null, null));
			settings.put("publicUse", publicUse);
			Map<String, Object> total = new LinkedHashMap<>();
			total.put("usageLimit", current(Source.ENGINE_TOTAL, null, null));
			settings.put("engineTotal", total);
			result.put("engineUsage", settings);
		}
	}

	static UsageLimit normalize(UsageLimit limit) {
		if (limit == null || limit.unlimited())
			return limit;
		var rules = limit.rules().stream().filter(rule -> rule.amount() != 0).toList();
		return rules.isEmpty() ? null : new UsageLimit(false, rules);
	}

	static void validate(PermissionTargetResource target, PermissionAllowedChanges allowed,
			PermissionChangeValidation validation, Context context) {
		PermissionChange change = validation.getChange();
		PermissionGrant to = change.getTo();
		boolean given = to != null && (to.isEngineUsageLimitGiven() || to.getEngineUsageLimit() != null);
		SubjectCategory category = change.getSubject().getCategory();
		boolean settings = category == SubjectCategory.PUBLIC || category == SubjectCategory.ENGINE;
		if (context == null) {
			if (given || category == SubjectCategory.ENGINE)
				validation.block(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT,
						"This target does not support engine usage limits");
			return;
		}
		if (settings && !allowed.isOwner()) {
			validation.block(PermissionProblem.ErrorCode.OWNERS_ONLY, "Only owners can set usage limits");
			return;
		}
		if (category == SubjectCategory.MODEL || (settings && to != null && to.getUsageLimit() != null)) {
			validation.block(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT,
					"Only a model has a public use limit and a total");
			return;
		}
		Source source = settings ? (category == SubjectCategory.PUBLIC ? Source.PUBLIC : Source.ENGINE_TOTAL)
				: category == SubjectCategory.TEAM ? Source.TEAM : Source.DIRECT;
		String id = settings ? null : validation.getSubjectId();
		String type = settings ? null : validation.getSubjectType();
		if (source == Source.DIRECT && type == null && given && to.getEngineUsageLimit() != null) {
			try {
				var types = QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
						"SELECT DISTINCT TYPE FROM SMSS_USER WHERE ID = ?", ps -> ps.setString(1, id),
						rs -> rs.getString(1));
				if (types.size() != 1 || ValueUtils.isBlank(types.get(0)))
					throw new IllegalArgumentException("Specify the person's login type for this usage limit");
				type = types.get(0);
			} catch (Exception failure) {
				validation.block(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT,
						"Specify the person's login type for this usage limit");
				return;
			}
		}
		UsageLimitKey key = context.key(source, id, type);
		UsageLimit current = context.current(source, id, type);
		boolean remove = change.getAction() == PermissionChange.Action.REMOVE;
		UsageLimit next = remove ? null : given ? normalize(to.getEngineUsageLimit()) : current;
		boolean keep = remove ? current == null && !(source == Source.DIRECT && context.hasDirect(id))
				: (!given || Objects.equals(current, next));
		validation.setEngineUsageLimitChange(key, current, next, keep);
		if (keep)
			return;
		if (!allowed.isOwner()) {
			validation.block(PermissionProblem.ErrorCode.OWNERS_ONLY, "Only owners can set usage limits");
			return;
		}
		try {
			if (next != null)
				SecurityEngineUsageLimitUtils.validate(key, next);
		} catch (IllegalArgumentException failure) {
			validation.block(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT, failure.getMessage());
		}
	}

	static void write(Connection connection, PermissionChangeValidation change, Pair<String, String> by)
			throws Exception {
		UsageLimitKey key = change.getEngineLimitKey();
		if (key == null || change.keepsEngineUsageLimit())
			return;
		if (key.source() == Source.DIRECT
				&& (change.getChange().getAction() == PermissionChange.Action.REMOVE || key.subjectType() == null)) {
			SecurityEngineUsageLimitUtils.deleteDirectPolicies(connection, key.engineId(), key.subjectId());
		} else {
			SecurityEngineUsageLimitUtils.setLimit(connection, key, change.getEngineUsageLimit(), by);
		}
	}

	static void audit(User actor, List<PermissionBatchValidation> batches, boolean admin) {
		for (var batch : batches)
			for (var change : batch.getChanges()) {
				if (change.getEngineLimitKey() != null && !change.keepsEngineUsageLimit()
						&& (change.getCurrentEngineUsageLimit() != null || change.getEngineUsageLimit() != null)) {
					SecurityEngineUsageLimitUtils.auditChange(actor, change.getEngineLimitKey(),
							change.getCurrentEngineUsageLimit(), change.getEngineUsageLimit(), admin);
				}
			}
	}
}
