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

import static prerna.usertracking.AuditCategory.AUDIT;
import static prerna.usertracking.AuditCategory.AUTH;
import static prerna.usertracking.AuditCategory.AUTHZ;
import static prerna.usertracking.AuditCategory.AUTOMATION;
import static prerna.usertracking.AuditCategory.CONFIG;
import static prerna.usertracking.AuditCategory.CREDENTIAL;
import static prerna.usertracking.AuditCategory.ENGINE;
import static prerna.usertracking.AuditCategory.GROUP_ADMIN;
import static prerna.usertracking.AuditCategory.INSIGHT;
import static prerna.usertracking.AuditCategory.JOB;
import static prerna.usertracking.AuditCategory.PROJECT;
import static prerna.usertracking.AuditCategory.SKILL;
import static prerna.usertracking.AuditCategory.USAGE_LIMIT;
import static prerna.usertracking.AuditCategory.USER_ADMIN;
import static prerna.usertracking.AuditCategory.WORKSPACE;
import static prerna.usertracking.AuditSeverity.HIGH;
import static prerna.usertracking.AuditSeverity.LOW;
import static prerna.usertracking.AuditSeverity.MEDIUM;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Every kind of event the audit trail records, stored in
 * {@code USER_AUDIT_EVENTS.EVENT_TYPE}. Each type names its category and the
 * severity it is recorded at; an event that does not succeed is recorded at
 * least at {@link AuditSeverity#MEDIUM}, and an event can set a higher severity
 * itself. A request that is denied is recorded as
 * {@link #AUTHORIZATION_DENIED}, with the action it tried as its
 * {@code ACTION}.
 */
public enum AuditEventType {

	// logging in and out

	/** A user logged in */
	LOGIN(AUTH, LOW),
	/** A user logged out, or their session ended */
	LOGOUT(AUTH, LOW),
	/** A login did not succeed */
	LOGIN_FAILED(AUTH, MEDIUM),

	// access

	/** A request was refused for lack of access */
	AUTHORIZATION_DENIED(AUTHZ, HIGH),
	/** A person or team was given access to a project, an engine or an insight */
	PERMISSION_ADD(AUTHZ, MEDIUM),
	/** A person's or team's access was changed */
	PERMISSION_UPDATE(AUTHZ, MEDIUM),
	/** A person's or team's access was taken away, or ended */
	PERMISSION_DELETE(AUTHZ, MEDIUM),
	/** Access was given to many people or resources at once */
	PERMISSION_BULK_GRANT(AUTHZ, HIGH),
	/** Permission batches were saved, recorded as one summary */
	PERMISSION_BATCH_APPLY(AUTHZ, HIGH),
	/** An access request was approved */
	ACCESS_REQUEST_APPROVE(AUTHZ, MEDIUM),
	/** An access request was rejected */
	ACCESS_REQUEST_REJECT(AUTHZ, MEDIUM),

	// model usage limits

	/** A model usage limit was set, changed or removed */
	USAGE_LIMIT_UPDATE(USAGE_LIMIT, MEDIUM),

	// user accounts

	/** A user account was created */
	USER_CREATE(USER_ADMIN, MEDIUM),
	/** A user account was changed */
	USER_UPDATE(USER_ADMIN, MEDIUM),
	/** A user account was deleted */
	USER_DELETE(USER_ADMIN, HIGH),
	/** A user account was unlocked */
	USER_ACTIVATE(USER_ADMIN, MEDIUM),
	/** A user account was locked */
	USER_DEACTIVATE(USER_ADMIN, HIGH),
	/** A user's roles changed, such as admin, publisher or exporter */
	USER_ROLE_UPDATE(USER_ADMIN, HIGH),
	/** A user's password was reset or changed */
	USER_PASSWORD_RESET(USER_ADMIN, HIGH),

	// teams

	/** A team was created */
	GROUP_CREATE(GROUP_ADMIN, MEDIUM),
	/** A team was changed */
	GROUP_UPDATE(GROUP_ADMIN, MEDIUM),
	/** A team was deleted */
	GROUP_DELETE(GROUP_ADMIN, HIGH),
	/** A member was added to a custom team */
	GROUP_MEMBER_ADD(GROUP_ADMIN, MEDIUM),
	/** A member was removed from a custom team */
	GROUP_MEMBER_REMOVE(GROUP_ADMIN, MEDIUM),
	/** A manager was added to a custom team */
	GROUP_MANAGER_ADD(GROUP_ADMIN, HIGH),
	/** A manager was removed from a custom team */
	GROUP_MANAGER_REMOVE(GROUP_ADMIN, HIGH),

	// API keys

	/** An API key was created */
	API_KEY_CREATE(CREDENTIAL, HIGH),
	/** An API key was deleted */
	API_KEY_DELETE(CREDENTIAL, HIGH),

	// projects

	/** A project was created */
	PROJECT_CREATE(PROJECT, LOW),
	/**
	 * A project was changed. No code records this type; it is kept so stored events
	 * of this type still read.
	 */
	@Deprecated
	PROJECT_UPDATE(PROJECT, MEDIUM),
	/** A project was renamed */
	PROJECT_RENAME(PROJECT, MEDIUM),
	/** A project's display name was changed */
	PROJECT_DISPLAY_NAME_UPDATE(PROJECT, LOW),
	/** A project was made public or private */
	PROJECT_GLOBAL_UPDATE(PROJECT, HIGH),
	/** A project's discoverable setting was turned on or off */
	PROJECT_DISCOVERABLE_UPDATE(PROJECT, MEDIUM),
	/** A project was made a template, or stopped being one */
	PROJECT_TEMPLATE_UPDATE(PROJECT, MEDIUM),
	/** A project's metadata was set or loaded */
	PROJECT_METADATA_UPDATE(PROJECT, LOW),
	/** A project's app blocks were saved */
	PROJECT_BLOCKS_UPDATE(PROJECT, LOW),
	/** A project was deleted */
	PROJECT_DELETE(PROJECT, HIGH),
	/** A project was uploaded */
	PROJECT_UPLOAD(PROJECT, MEDIUM),
	/** An existing project's files were replaced by an upload */
	PROJECT_REPLACE(PROJECT, MEDIUM),
	/** A project was opened; recorded when it is refused */
	PROJECT_VIEW(PROJECT, LOW),

	// insights

	/** An insight was changed */
	INSIGHT_UPDATE(INSIGHT, LOW),
	/** An insight was deleted */
	INSIGHT_DELETE(INSIGHT, MEDIUM),

	// workspaces

	/** A workspace was created */
	WORKSPACE_CREATE(WORKSPACE, LOW),
	/** A workspace was changed */
	WORKSPACE_UPDATE(WORKSPACE, LOW),
	/** A workspace was deleted */
	WORKSPACE_DELETE(WORKSPACE, MEDIUM),

	// engines

	/** An engine was created or uploaded */
	ENGINE_CREATE(ENGINE, LOW),
	/**
	 * An engine was changed. No code records this type; it is kept so stored events
	 * of this type still read.
	 */
	@Deprecated
	ENGINE_UPDATE(ENGINE, MEDIUM),
	/** An engine was renamed */
	ENGINE_RENAME(ENGINE, MEDIUM),
	/** An engine's display name was changed */
	ENGINE_DISPLAY_NAME_UPDATE(ENGINE, LOW),
	/** An engine was made public or private */
	ENGINE_GLOBAL_UPDATE(ENGINE, HIGH),
	/** An engine's discoverable setting was turned on or off */
	ENGINE_DISCOVERABLE_UPDATE(ENGINE, MEDIUM),
	/** An engine's metadata was set or loaded */
	ENGINE_METADATA_UPDATE(ENGINE, LOW),
	/** A user showed or hid an engine in their own lists */
	ENGINE_VISIBILITY_UPDATE(ENGINE, LOW),
	/** An engine was deleted */
	ENGINE_DELETE(ENGINE, HIGH),
	/** A model engine was created */
	MODEL_CREATE(ENGINE, LOW),
	/** A model served by KServe was started (its InferenceService's stop annotation lifted) */
	MODEL_SERVING_START(ENGINE, MEDIUM),
	/** A model served by KServe was stopped (its pods deleted, the GPU freed) */
	MODEL_SERVING_STOP(ENGINE, MEDIUM),
	/** A model was defined for KServe from the serving catalog */
	MODEL_SERVING_DEFINE(ENGINE, MEDIUM),
	/** A model SEMOSS defined for KServe was removed */
	MODEL_SERVING_REMOVE(ENGINE, HIGH),
	/** A model engine was linked to, or unlinked from, a model served by KServe */
	MODEL_SERVING_ENGINE_LINK(ENGINE, LOW),
	/** A vector database engine was created */
	VECTOR_CREATE(ENGINE, LOW),
	/** A storage engine was created */
	STORAGE_CREATE(ENGINE, LOW),
	/** A function engine was created */
	FUNCTION_CREATE(ENGINE, LOW),
	/** A guardrail engine was created */
	GUARDRAIL_CREATE(ENGINE, LOW),
	/** SQL or a query struct changed a database */
	DATABASE_WRITE(ENGINE, MEDIUM),

	// automations

	/** An automation was created */
	AUTOMATION_CREATE(AUTOMATION, LOW),
	/** An automation was saved */
	AUTOMATION_UPDATE(AUTOMATION, LOW),
	/** An automation run started */
	AUTOMATION_RUN_START(AUTOMATION, MEDIUM),
	/** An automation run ended, successfully or not */
	AUTOMATION_RUN_END(AUTOMATION, LOW),
	/** An automation run was cancelled */
	AUTOMATION_RUN_CANCEL(AUTOMATION, LOW),
	/** A paused automation run was resumed */
	AUTOMATION_RUN_RESUME(AUTOMATION, LOW),
	/** An action an automation's agent asked for was approved or declined */
	AUTOMATION_ACTION_RESOLVE(AUTOMATION, MEDIUM),
	/** An automation's agent run was stopped */
	AUTOMATION_AGENT_STOP(AUTOMATION, LOW),

	// skills

	/** A skill was created or cloned */
	SKILL_CREATE(SKILL, LOW),
	/** A skill was changed */
	SKILL_UPDATE(SKILL, LOW),
	/** A skill was deleted */
	SKILL_DELETE(SKILL, HIGH),
	/** A skill was attached to a workspace */
	SKILL_ATTACH(SKILL, LOW),
	/** A skill was detached from a workspace */
	SKILL_DETACH(SKILL, LOW),

	// scheduled jobs

	/** A job was scheduled */
	JOB_CREATE(JOB, MEDIUM),
	/** A scheduled job was changed */
	JOB_UPDATE(JOB, MEDIUM),
	/** A scheduled job was deleted */
	JOB_DELETE(JOB, HIGH),
	/** A scheduled job was paused */
	JOB_PAUSE(JOB, LOW),
	/** A paused job was resumed */
	JOB_RESUME(JOB, LOW),

	// platform configuration

	/** The platform's configuration was changed */
	CONFIG_UPDATE(CONFIG, HIGH),

	// the audit trail

	/** The audit trail was read */
	AUDIT_QUERY(AUDIT, LOW),
	/** A query of the audit trail was prepared, before it was read */
	AUDIT_QUERY_REQUEST(AUDIT, LOW),
	/** The audit trail was exported */
	AUDIT_EXPORT(AUDIT, HIGH),
	/** The audit trail's chain of hashes was verified */
	AUDIT_VERIFY(AUDIT, LOW);

	/**
	 * The changes to a project's or an engine's own settings: its name, display
	 * name, public and discoverable access, template setting and metadata, and a
	 * model engine's link to a served model. Saving a project's blocks and a user
	 * showing or hiding an engine in their own lists are not settings of the
	 * project or engine.
	 */
	public static final Set<AuditEventType> SETTING_CHANGES = Collections.unmodifiableSet(
			EnumSet.of(PROJECT_RENAME, PROJECT_DISPLAY_NAME_UPDATE, PROJECT_GLOBAL_UPDATE, PROJECT_DISCOVERABLE_UPDATE,
					PROJECT_TEMPLATE_UPDATE, PROJECT_METADATA_UPDATE, ENGINE_RENAME, ENGINE_DISPLAY_NAME_UPDATE,
					ENGINE_GLOBAL_UPDATE, ENGINE_DISCOVERABLE_UPDATE, ENGINE_METADATA_UPDATE, MODEL_SERVING_ENGINE_LINK));

	private final AuditCategory category;
	private final AuditSeverity severity;

	AuditEventType(AuditCategory category, AuditSeverity severity) {
		this.category = category;
		this.severity = severity;
	}

	/**
	 * Reads an event type stored by its name.
	 *
	 * @param name the stored name, or null
	 * @return the event type with the name, or null when no type has it
	 */
	public static AuditEventType find(String name) {
		for (AuditEventType type : values()) {
			if (type.name().equals(name)) {
				return type;
			}
		}
		return null;
	}

	/**
	 * The part of the platform the event belongs to.
	 *
	 * @return the category every event of this type is recorded in
	 */
	public AuditCategory getCategory() {
		return this.category;
	}

	/**
	 * The severity an event of this type is recorded at when it succeeds and sets
	 * none itself.
	 *
	 * @return the type's severity
	 */
	public AuditSeverity getSeverity() {
		return this.severity;
	}
}
