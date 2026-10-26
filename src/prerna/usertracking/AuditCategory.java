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

/**
 * The part of the platform an audit event belongs to, stored in
 * {@code USER_AUDIT_EVENTS.CATEGORY}. Each {@link AuditEventType} names its
 * category.
 */
public enum AuditCategory {

	/** Logging in and out */
	AUTH,
	/** Access: permissions, access requests and denied requests */
	AUTHZ,
	/** Model usage limits */
	USAGE_LIMIT,
	/** User accounts, as admins manage them */
	USER_ADMIN,
	/** Teams, with their members and managers */
	GROUP_ADMIN,
	/** API keys */
	CREDENTIAL,
	/** Projects, which the UI calls apps */
	PROJECT,
	/** Insights in a project */
	INSIGHT,
	/** Workspaces */
	WORKSPACE,
	/**
	 * Engines of every kind: databases, models, vectors, storages, functions,
	 * guardrails
	 */
	ENGINE,
	/** Automations and their runs */
	AUTOMATION,
	/** Skills */
	SKILL,
	/** Scheduled jobs */
	JOB,
	/** Platform configuration */
	CONFIG,
	/** Reads and checks of the audit trail itself */
	AUDIT;
}
