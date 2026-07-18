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
package prerna.reactor.notification;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.NotificationConstants;
import prerna.util.Utility;

public class FetchNotificationsReactor extends AbstractReactor {
	private static final String SCOPE_TYPE = "scopeType";
	private static final String SCOPE_ID = "scopeId";

	public FetchNotificationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), SCOPE_TYPE,
				SCOPE_ID };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		if (!Utility.isNotificationDatabaseEnabled()) {
			throw new IllegalArgumentException("Notifications are not enabled on this instance");
		}
		organizeKeys();
		User user = this.insight.getUser();
		String limit = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offset = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		String scopeType = normalizeScopeType(this.keyValue.get(SCOPE_TYPE));
		String scopeId = this.keyValue.get(SCOPE_ID);
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account to retrieve the function engine files",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
			throwAnonymousUserError();
		}
		if (NotificationConstants.FetchScope.APP.equals(scopeType)
				&& !SecurityProjectUtils.userCanViewProject(user, scopeId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		List<Map<String, Object>> allNotifications = NotificationDbUtils.fetchNotifications(user, scopeType, scopeId, limit,
				offset);
		if (!allNotifications.isEmpty()) {
			NotificationDbUtils.resetNotificationActionType(user, scopeType, scopeId);
		}

		return new NounMetadata(allNotifications, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Fetch all user notifications";
	}

	private String normalizeScopeType(String scopeType) {
		String normalized = scopeType == null || scopeType.trim().isEmpty() ? NotificationConstants.FetchScope.ALL
				: scopeType.trim().toUpperCase();
		if (!NotificationConstants.FetchScope.isValid(normalized)) {
			throw new IllegalArgumentException("Notification scopeType must be ALL, SYSTEM, or APP");
		}
		if (NotificationConstants.FetchScope.APP.equals(normalized)
				&& (this.keyValue.get(SCOPE_ID) == null || this.keyValue.get(SCOPE_ID).trim().isEmpty())) {
			throw new IllegalArgumentException("Notification scopeId is required when scopeType is APP");
		}
		return normalized;
	}
}
