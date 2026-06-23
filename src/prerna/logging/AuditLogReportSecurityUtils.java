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
package prerna.logging;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;

/**
 * Shared access-control logic for the audit log report reactors. Centralizes
 * the checks originally written inline in {@link AuditLogReportReactor} so
 * every reactor that exposes audit log data enforces the exact same rules:
 * <ul>
 * <li>at least one of project/engine/room must be provided</li>
 * <li>room access is validated against the calling user</li>
 * <li>the caller must have access to the project/engine (or it must be
 * global)</li>
 * <li>non-owners are restricted to their own logs</li>
 * </ul>
 */
public class AuditLogReportSecurityUtils {

	private AuditLogReportSecurityUtils() {

	}

	/**
	 * Result of an audit log access check.
	 */
	public static class AuditLogAccess {

		private final boolean owner;
		private final String filterUserId;

		public AuditLogAccess(boolean owner, String filterUserId) {
			this.owner = owner;
			this.filterUserId = filterUserId;
		}

		/**
		 * @return whether the caller owns the project/engine being queried
		 */
		public boolean isOwner() {
			return owner;
		}

		/**
		 * @return the user id the logs should be scoped to - the owner-selected filter
		 *         when the caller is an owner, otherwise the caller's own id
		 */
		public String getFilterUserId() {
			return filterUserId;
		}
	}

	/**
	 * Validate that the calling user is allowed to view audit logs for the given
	 * project/engine/room and resolve the user id the logs must be scoped to.
	 *
	 * @param insight              the current insight (provides the calling user)
	 * @param projectId            the project being queried (may be null/blank)
	 * @param engineId             the engine being queried (may be null/blank)
	 * @param roomId               the room being queried (may be null/blank)
	 * @param selectedFilterUserId the user id the caller asked to filter on - only
	 *                             honored when the caller is an owner
	 * @return the resolved {@link AuditLogAccess}
	 * @throws IllegalArgumentException if no id is provided or the caller does not
	 *                                  have access
	 */
	public static AuditLogAccess authorize(Insight insight, String projectId, String engineId, String roomId,
			String selectedFilterUserId) {
		User user = insight.getUser();

		// validate we have values
		if (isBlank(projectId) && isBlank(engineId) && isBlank(roomId)) {
			throw new IllegalArgumentException("Must provide engine, project, or a room id");
		}

		// if not project or engine but a room
		// then we need to validate you have access to the room
		if (isBlank(projectId) && isBlank(engineId) && !isBlank(roomId)) {
			// this will throw an error if the room does not exist for this user
			RoomUtils.getOrLoadRoom(roomId, insight);
		}

		if (!isBlank(roomId) && !ModelInferenceLogsUtils.doCheckRoomExists(roomId)) {
			throw new IllegalArgumentException("Room ID is not valid");
		}

		String userId = user.getPrimaryLoginToken().getId();

		// if you are using a project or an engine
		// let us check if you are the owner of either of these
		boolean userIsOwner = false;
		if (!isBlank(projectId)) {
			Integer userPermissionLvl = SecurityProjectUtils.getUserProjectPermission(userId, projectId);
			if (userPermissionLvl == null && !SecurityProjectUtils.projectIsGlobal(projectId)) {
				throw new IllegalArgumentException(
						"Project id '" + projectId + "' does not exist or user does not have access");
			}
			if (userPermissionLvl != null && AccessPermissionEnum.isOwner(userPermissionLvl)) {
				userIsOwner = true;
			}
		}
		// only need to check if not already owner of the project
		if (!userIsOwner && !isBlank(engineId)) {
			Integer userPermissionLvl = SecurityEngineUtils.getUserEnginePermission(userId, engineId);
			if (userPermissionLvl == null && !SecurityEngineUtils.engineIsGlobal(engineId)) {
				throw new IllegalArgumentException(
						"Engine id '" + engineId + "' does not exist or user does not have access");
			}
			if (userPermissionLvl != null && AccessPermissionEnum.isOwner(userPermissionLvl)) {
				userIsOwner = true;
			}
		}

		// owners may filter on any user; everyone else is restricted to their own logs
		String filterUserId = userIsOwner ? selectedFilterUserId : userId;
		return new AuditLogAccess(userIsOwner, filterUserId);
	}

	private static boolean isBlank(String value) {
		return value == null || value.isBlank();
	}

}
