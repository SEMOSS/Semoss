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
package prerna.reactor.notification;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.NotificationConstants;
import prerna.util.Utility;

/** Fetches notifications scoped to the project bound to the current insight. */
public class FetchAppNotificationsReactor extends AbstractReactor {

	public FetchAppNotificationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		if (!Utility.isNotificationDatabaseEnabled()) {
			throw new IllegalArgumentException("Notifications are not enabled on this instance");
		}
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
			throwAnonymousUserError();
		}

		String projectId = getInsightProjectId();
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		List<Map<String, Object>> notifications = NotificationDbUtils.fetchNotifications(user,
				NotificationConstants.FetchScope.APP, projectId, this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()),
				this.keyValue.get(ReactorKeysEnum.OFFSET.getKey()));
		if (!notifications.isEmpty()) {
			NotificationDbUtils.resetNotificationActionType(user, NotificationConstants.FetchScope.APP, projectId);
		}
		return new NounMetadata(notifications, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Fetch notifications for the project bound to the current insight";
	}

	private String getInsightProjectId() {
		String projectId = this.insight.getContextProjectId();
		if (StringUtils.isBlank(projectId)) {
			throw new IllegalStateException("Current insight is not associated with an app project");
		}
		return projectId;
	}
}
