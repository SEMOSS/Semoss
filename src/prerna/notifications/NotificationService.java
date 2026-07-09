/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
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
package prerna.notifications;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.Insight;
import prerna.util.NotificationConstants;

/** Public notification creation boundary for application-facing flows. */
public final class NotificationService {

	private NotificationService() {
	}

	public static String createAppAnnouncement(User creator, String projectId, String title, String message,
			String priority) {
		if (!SecurityProjectUtils.userCanEditProject(creator, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user is not an editor of the project");
		}
		return createAppNotification(projectId, NotificationConstants.Audience.APP_MEMBERS, null, null,
				creator == null ? null : User.getSingleLogginName(creator), title, message, priority);
	}

	/**
	 * Notifies the authenticated insight user within the insight's active app context.
	 * This has no public reactor because callers must not be able to target other users.
	 */
	public static String createAppUserNotification(Insight insight, String title, String message, String priority) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight is required to create an app user notification");
		}
		String projectId = requireValue(insight.getContextProjectId(), "app context project id");
		User user = insight.getUser();
		if (user == null || user.isAnonymous()) {
			throw new IllegalArgumentException("A signed-in insight user is required to create an app user notification");
		}
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Insight user does not have access to the active app project");
		}

		AuthProvider provider = user.getPrimaryLogin();
		if (provider == null) {
			throw new IllegalArgumentException("Insight user does not have a primary login provider");
		}
		AccessToken token = user.getAccessToken(provider);
		if (token == null) {
			throw new IllegalArgumentException("Insight user does not have an access token for the primary login provider");
		}
		return createAppUserNotification(projectId, requireValue(token.getId(), "user id"), provider.toString(), title,
				message, priority);
	}

	private static String createAppUserNotification(String projectId, String userId, String userType, String title,
			String message, String priority) {
		return createAppNotification(projectId, NotificationConstants.Audience.USER, requireValue(userId, "user id"),
				requireValue(userType, "user type"), userId, title, message, priority);
	}

	private static String createAppNotification(String projectId, String audienceType, String audienceId,
			String audienceUserType, String createdBy, String title, String message, String priority) {
		String normalizedProjectId = requireValue(projectId, "project id");
		String normalizedTitle = requireValue(title, "title");
		if (normalizedTitle.length() > 255) {
			throw new IllegalArgumentException("Notification title cannot exceed 255 characters");
		}
		String normalizedMessage = requireValue(message, "message");
		String normalizedPriority = normalizePriority(priority);
		return NotificationDbUtils.insertNotificationEvent(NotificationConstants.Kind.INFO,
				NotificationConstants.Type.ANNOUNCEMENT, NotificationConstants.Scope.APP, normalizedProjectId,
				audienceType, audienceId, audienceUserType, normalizedTitle, normalizedMessage, normalizedPriority,
				NotificationConstants.DisplaySurface.BELL, NotificationConstants.Source.PROJECT, normalizedProjectId,
				NotificationConstants.Target.APP, normalizedProjectId, null, null, NotificationConstants.Status.ACTIVE,
				null, null, createdBy);
	}

	private static String normalizePriority(String priority) {
		String normalized = StringUtils.defaultIfBlank(priority, NotificationConstants.Priority.NORMAL).trim()
				.toUpperCase();
		if (!NotificationConstants.Priority.isValid(normalized)) {
			throw new IllegalArgumentException("Notification priority must be LOW, NORMAL, HIGH, or URGENT");
		}
		return normalized;
	}

	private static String requireValue(String value, String label) {
		if (StringUtils.isBlank(value)) {
			throw new IllegalArgumentException("Notification " + label + " is required");
		}
		return value.trim();
	}
}
