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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionSubject.SubjectCategory;
import prerna.auth.access.PermissionTargetResource;
import prerna.auth.access.PermissionTargetResource.TargetResourceType;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditStatus;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserAuditTrailUtils.AuditEvent;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/**
 * Adapts committed permission batches to the existing audit event contract: one
 * event per change of access, and one {@link AuditEventType#USAGE_LIMIT_UPDATE}
 * event per usage limit a change sets, changes or removes.
 */
final class PermissionBatchAudit {

	private static final Logger LOGGER = LogManager.getLogger(PermissionBatchAudit.class);

	private PermissionBatchAudit() {
	}

	static void recordChanges(User user, List<PermissionBatchValidation> batches, PermissionBatchOptions options) {
		try {
			if (!Utility.isUserTrackingEnabled()) {
				return;
			}
			for (PermissionBatchValidation batch : batches) {
				AuditedTarget target = describe(batch.getTarget());
				for (PermissionChangeValidation change : batch.getChanges()) {
					// a model's public use and total have no access to change, only a limit
					if (!options.hasSummaryAudit() && !isModelLimit(change)) {
						recordChange(user, options, target, change);
					}
					// usage limit changes are recorded one by one, even under a summary,
					// since each changes what someone may spend
					if (change.changesUsageLimit()) {
						recordUsageLimitChange(user, options, target, change);
					}
				}
			}
			if (options.getApprovedRequests() != null) {
				AuditedTarget approved = describe(options.getApprovedTarget());
				for (Map<String, ?> request : options.getApprovedRequests()) {
					var event = event(user, options, AuditEventType.ACCESS_REQUEST_APPROVE, approved).subject(
							ValueUtils.toStringOrNull(request.get("userid")),
							ValueUtils.toStringOrNull(request.get("type")), null);
					String permission = ValueUtils.toStringOrNull(request.get("permission"));
					if (permission != null) {
						event.newValue(Map.of("permission", permission));
					}
					String requestId = ValueUtils.toStringOrNull(request.get("requestid"));
					if (requestId != null) {
						event.details(Map.of("requestId", requestId));
					}
					UserAuditTrailUtils.recordEvent(event);
				}
			}
		} catch (Exception failure) {
			LOGGER.warn("Unable to audit committed permission changes", failure);
		}
	}

	static void recordDenials(User user, PermissionTargetResource target, List<String> requestIds,
			PermissionBatchOptions options) {
		try {
			if (!Utility.isUserTrackingEnabled()) {
				return;
			}
			AuditedTarget denied = describe(target);
			for (String requestId : requestIds) {
				UserAuditTrailUtils.recordEvent(event(user, options, AuditEventType.ACCESS_REQUEST_REJECT, denied)
						.details(Map.of("requestId", requestId)));
			}
		} catch (Exception failure) {
			LOGGER.warn("Unable to audit committed access-request decisions", failure);
		}
	}

	private static void recordChange(User user, PermissionBatchOptions options, AuditedTarget target,
			PermissionChangeValidation validation) {
		PermissionChange change = validation.getChange();
		AuditEventType eventType = switch (change.getAction()) {
		case ADD -> AuditEventType.PERMISSION_ADD;
		case CHANGE -> AuditEventType.PERMISSION_UPDATE;
		case REMOVE -> AuditEventType.PERMISSION_DELETE;
		};
		Map<String, Object> details = new LinkedHashMap<>();
		PermissionGrant grant = change.getTo();
		if (grant != null && grant.getEndDate() != null) {
			details.put("endDate", grant.getEndDate());
		}
		AuditEvent event;
		if (isGroup(target.resource())) {
			boolean managers = target.resource().getResourceType() == TargetResourceType.TEAM_MANAGERS;
			boolean added = change.getAction() == PermissionChange.Action.ADD;
			AuditEventType teamChange = managers
					? (added ? AuditEventType.GROUP_MANAGER_ADD : AuditEventType.GROUP_MANAGER_REMOVE)
					: (added ? AuditEventType.GROUP_MEMBER_ADD : AuditEventType.GROUP_MEMBER_REMOVE);
			event = event(user, options, teamChange, target).subject(validation.getSubjectId(),
					validation.getSubjectType(), null);
			details.put("groupType", SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE);
		} else {
			event = event(user, options, eventType, target);
			if (change.getSubject().getCategory() == SubjectCategory.TEAM) {
				details.put("groupId", validation.getSubjectId());
				details.put("groupType", validation.getSubjectType());
			} else {
				String subjectType = validation.getSubjectType();
				// Approval writes intentionally match by ID; retain the request's provider in
				// the audit.
				if (subjectType == null && options.getApprovedRequests() != null) {
					for (Map<String, ?> request : options.getApprovedRequests()) {
						if (validation.getSubjectId().equals(ValueUtils.toStringOrNull(request.get("userid")))) {
							subjectType = ValueUtils.toStringOrNull(request.get("type"));
							break;
						}
					}
				}
				event.subject(validation.getSubjectId(), subjectType, null);
				details.put("granteeId", validation.getSubjectId());
				if (subjectType != null) {
					details.put("granteeType", subjectType);
				}
			}
			if (validation.getCurrentLevelId() != null) {
				event.oldValue(Map.of("permission",
						AccessPermissionEnum.getPermissionValueById(validation.getCurrentLevelId())));
			}
			if (change.getAction() != PermissionChange.Action.REMOVE && validation.getLevelId() != null) {
				event.newValue(
						Map.of("permission", AccessPermissionEnum.getPermissionValueById(validation.getLevelId())));
			}
		}
		UserAuditTrailUtils.recordEvent(event.details(details));
	}

	/**
	 * Records the usage limit a change sets, changes or removes on a model: a
	 * person's Direct Limit or a team's Team Limit on their grant, or the model's
	 * Public Use Limit or Model Total. A grant's removal records its limit removed
	 * with it.
	 */
	private static void recordUsageLimitChange(User user, PermissionBatchOptions options, AuditedTarget target,
			PermissionChangeValidation validation) {
		ModelUsageLimitKey key = validation.getLimitKey();
		AuditEvent event = event(user, options, AuditEventType.USAGE_LIMIT_UPDATE, target)
				.oldValue(ModelUsageLimitAudit.value(validation.getCurrentUsageLimit()))
				.newValue(ModelUsageLimitAudit.value(validation.getUsageLimit()));
		Map<String, Object> details = ModelUsageLimitAudit.details(key);
		if (key.isDirect()) {
			event.subject(validation.getSubjectId(), validation.getSubjectType(), null);
		}
		if (validation.getChange().getAction() == PermissionChange.Action.REMOVE) {
			details.put("removedWithGrant", true);
		}
		UserAuditTrailUtils.recordEvent(event.details(details));
	}

	/**
	 * Whether a change is to a model's public use or total, which have only a usage
	 * limit.
	 */
	private static boolean isModelLimit(PermissionChangeValidation validation) {
		SubjectCategory category = validation.getChange().getSubject().getCategory();
		return category == SubjectCategory.PUBLIC || category == SubjectCategory.MODEL;
	}

	private static AuditEvent event(User user, PermissionBatchOptions options, AuditEventType type,
			AuditedTarget target) {
		PermissionTargetResource resource = target.resource();
		String projectId = switch (resource.getResourceType()) {
		case PROJECT -> resource.getId();
		case INSIGHT -> resource.getProjectId();
		default -> null;
		};
		var event = new AuditEvent().eventType(type).status(AuditStatus.SUCCESS).actorUser(user)
				.actorIsAdmin(options.isAdminMode() ? true : null)
				.target(target.type(), resource.getId(), target.name()).context(projectId,
						resource.getResourceType() == TargetResourceType.ENGINE ? resource.getId() : null,
						resource.getResourceType() == TargetResourceType.INSIGHT ? resource.getId() : null, null);
		if (options.getGrantor() != null) {
			event.actor(options.getGrantor().getValue0(), options.getGrantor().getValue1(), null);
		}
		return event;
	}

	/**
	 * A batch's target as the audit trail records it.
	 *
	 * @param resource the target
	 * @param type     what it is: an engine by its kind, such as a model
	 * @param name     its name, or null when it could not be read
	 */
	private record AuditedTarget(PermissionTargetResource resource, AuditTargetType type, String name) {
	}

	/**
	 * Reads what a batch's target is, once for the batch: an engine by its kind,
	 * such as a model, and a project, an engine or an insight with its name. A
	 * team, and its managers, is a team named by its id. When the catalog cannot be
	 * read, the target is recorded by what it is in general, with no name.
	 */
	private static AuditedTarget describe(PermissionTargetResource target) {
		try {
			return switch (target.getResourceType()) {
			case PROJECT -> new AuditedTarget(target, AuditTargetType.PROJECT,
					SecurityProjectUtils.getProjectDisplayNameForId(target.getId()));
			case ENGINE -> new AuditedTarget(target, SecurityEngineUtils.getAuditTargetType(target.getId()),
					SecurityEngineUtils.getEngineDisplayNameForId(target.getId()));
			case INSIGHT -> new AuditedTarget(target, AuditTargetType.INSIGHT,
					SecurityQueryUtils.getInsightNameForId(target.getProjectId(), target.getId()));
			case TEAM, TEAM_MANAGERS -> new AuditedTarget(target, AuditTargetType.GROUP, target.getId());
			};
		} catch (Exception e) {
			LOGGER.warn("Unable to read the kind and name of {} for the audit trail", target.describe(), e);
			return new AuditedTarget(target, generalType(target), isGroup(target) ? target.getId() : null);
		}
	}

	/** What a batch's target is in general; a team and its managers are a team */
	private static AuditTargetType generalType(PermissionTargetResource target) {
		return switch (target.getResourceType()) {
		case PROJECT -> AuditTargetType.PROJECT;
		case ENGINE -> AuditTargetType.ENGINE;
		case INSIGHT -> AuditTargetType.INSIGHT;
		case TEAM, TEAM_MANAGERS -> AuditTargetType.GROUP;
		};
	}

	private static boolean isGroup(PermissionTargetResource target) {
		return target.getResourceType() == TargetResourceType.TEAM
				|| target.getResourceType() == TargetResourceType.TEAM_MANAGERS;
	}
}
