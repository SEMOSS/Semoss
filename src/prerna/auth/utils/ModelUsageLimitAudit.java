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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserAuditTrailUtils.AuditEvent;
import prerna.util.Utility;

/**
 * Records changes to model usage limits in the audit trail: one
 * {@value #EVENT_TYPE} event for each limit a change leaves different, with the
 * limit before and after as {@code {"usageLimit": ...}} (null for no limit
 * defined) and which limit it is in {@code details.limitType}.
 *
 * <ul>
 * <li>Limits on a model, set through permission batches (a Direct Limit, a
 * Team Limit, the Public Use Limit and the Model Total), are recorded by
 * {@link PermissionBatchAudit} after the batch commits.</li>
 * <li>Limits that span models, set by admins (the General Platform Limit, a
 * Team Total and a User's Total Model Limit), are recorded through
 * {@link #recordAdminChange} after they are saved.</li>
 * </ul>
 */
final class ModelUsageLimitAudit {

	private static final Logger classLogger = LogManager.getLogger(ModelUsageLimitAudit.class);

	/** The event a usage limit change is recorded as */
	static final String EVENT_TYPE = "USAGE_LIMIT_UPDATE";
	/** The key of the old and new values that holds the limit */
	static final String USAGE_LIMIT = "usageLimit";

	private ModelUsageLimitAudit() {

	}

	/**
	 * Names the limit a key stands for, by the names the usage limit path uses.
	 *
	 * @param key where a limit is set
	 * @return {@code DIRECT}, {@code TEAM}, {@code PUBLIC}, {@code MODEL_TOTAL},
	 *         {@code TEAM_TOTAL}, {@code ALL_MODELS} or {@code PLATFORM}
	 */
	static String limitTypeOf(ModelUsageLimitKey key) {
		switch (key.type()) {
		case USER:
			return key.engineId() == null ? "ALL_MODELS" : "DIRECT";
		case TEAM:
			return key.engineId() == null ? "TEAM_TOTAL" : "TEAM";
		case PUBLIC:
			return "PUBLIC";
		case MODEL:
			return "MODEL_TOTAL";
		default:
			return "PLATFORM";
		}
	}

	/**
	 * Builds the old or new value of an event.
	 *
	 * @param limit a limit, or null for no limit defined
	 * @return {@code {"usageLimit": limit}}, null included
	 */
	static Map<String, Object> value(ModelUsageLimit limit) {
		Map<String, Object> value = new LinkedHashMap<>();
		value.put(USAGE_LIMIT, limit);
		return value;
	}

	/**
	 * Builds the event for one limit's change, for the caller to add who made it
	 * and where.
	 *
	 * @param key    where the limit is set
	 * @param before the limit before the change, or null for no limit defined
	 * @param after  the limit after the change, or null for no limit defined
	 * @return the event, with the limit before and after, and the limit type and,
	 *         for a team's limit, the team in its details
	 */
	static AuditEvent event(ModelUsageLimitKey key, ModelUsageLimit before, ModelUsageLimit after) {
		return new AuditEvent().eventType(EVENT_TYPE).action(EVENT_TYPE).status(UserAuditTrailUtils.STATUS_SUCCESS)
				.oldValue(value(before)).newValue(value(after)).details(details(key));
	}

	/**
	 * Builds the details of a limit's change, for the caller to add to.
	 *
	 * @param key where the limit is set
	 * @return the limit type and, for a team's limit or total, the team
	 */
	static Map<String, Object> details(ModelUsageLimitKey key) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("limitType", limitTypeOf(key));
		if (key.type() == ModelUsageLimitKey.Type.TEAM) {
			details.put("groupId", key.subjectId());
			details.put("groupType", key.subjectType());
		}
		return details;
	}

	/**
	 * Records a change an admin made to a limit that spans models: the General
	 * Platform Limit, a Team Total, or a person's User's Total Model Limit. Nothing
	 * is recorded when the limit is the same as before or user tracking is off, and
	 * a failure to record is logged, since the change is already saved.
	 *
	 * @param user   the admin who made the change, or null when the audit trail
	 *               reads it from the request
	 * @param key    where the limit is set
	 * @param before the limit before the change, or null for no limit defined
	 * @param after  the limit after the change, or null for no limit defined
	 */
	static void recordAdminChange(User user, ModelUsageLimitKey key, ModelUsageLimit before, ModelUsageLimit after) {
		try {
			if (!Utility.isUserTrackingEnabled() || PermissionBatchUtils.sameLimit(before, after)) {
				return;
			}
			AuditEvent event = event(key, before, after).actorUser(user).actorIsAdmin(true);
			switch (key.type()) {
			case USER:
				event.subject(key.subjectId(), key.subjectType(), null).target("USER", key.subjectId(), null);
				break;
			case TEAM:
				event.target("GROUP", key.subjectId(), key.subjectId());
				break;
			default:
				// the General Platform Limit applies to everyone with no other limit
				event.target("PLATFORM", null, null).severity(UserAuditTrailUtils.SEVERITY_HIGH);
				break;
			}
			UserAuditTrailUtils.recordEvent(event);
		} catch (Exception e) {
			classLogger.warn("Unable to audit a model usage limit change", e);
		}
	}

	/**
	 * Records the Direct Limits removed with a grant that ended, which SEMOSS removes
	 * itself when the person next reaches the model. A failure to record is logged,
	 * since the grant is already removed.
	 *
	 * @param engineId the model
	 * @param removed  the person's Direct Limits on the model, by key, as they were
	 *                 before the removal
	 */
	static void recordRemovedWithExpiredGrant(String engineId, Map<ModelUsageLimitKey, ModelUsageLimit> removed) {
		try {
			if (removed.isEmpty() || !Utility.isUserTrackingEnabled()) {
				return;
			}
			for (Map.Entry<ModelUsageLimitKey, ModelUsageLimit> entry : removed.entrySet()) {
				ModelUsageLimitKey key = entry.getKey();
				Map<String, Object> details = details(key);
				details.put("removedWithGrant", true);
				details.put("reason", "EXPIRED");
				UserAuditTrailUtils.recordEvent(event(key, entry.getValue(), null)
						.actor(UserAuditTrailUtils.ACTOR_TYPE_SYSTEM, UserAuditTrailUtils.ACTOR_TYPE_SYSTEM,
								UserAuditTrailUtils.ACTOR_TYPE_SYSTEM)
						.subject(key.subjectId(), key.subjectType(), null).target("ENGINE", engineId, null)
						.context(null, engineId, null, null).details(details));
			}
		} catch (Exception e) {
			classLogger.warn("Unable to audit the usage limits removed with an expired grant", e);
		}
	}
}
