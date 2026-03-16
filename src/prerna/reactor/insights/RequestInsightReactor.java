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
package prerna.reactor.insights;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EmailUtility;
import prerna.util.Utility;

public class RequestInsightReactor extends AbstractReactor {
	public RequestInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(),
				ReactorKeysEnum.PERMISSION.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String insightId = this.keyValue.get(this.keysToGet[1]);
		String permission = this.keyValue.get(this.keysToGet[2]);
		String requestComment = this.keyValue.get(this.keysToGet[3]);
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to request a insight",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String userId = token.getId();
		// check user permission for the insight
		Integer currentUserPermission = SecurityInsightUtils.getUserInsightPermission(userId, projectId, insightId);
		// make sure requesting new level of permission
		int requestPermission = -1;
		try {
			requestPermission = Integer.parseInt(permission);
		} catch (NumberFormatException ignore) {
			requestPermission = AccessPermissionEnum.getPermissionByValue(permission).getId();
		}
		if (currentUserPermission != null && requestPermission == currentUserPermission) {
			throw new IllegalArgumentException(
					"This user already has access to this project with the given permission level");
		}

		// check user pending permission
		Integer currentPendingUserPermission = SecurityInsightUtils.getUserAccessRequestInsightPermission(userId,
				projectId, insightId);
		if (currentPendingUserPermission != null && requestPermission == currentPendingUserPermission) {
			throw new IllegalArgumentException(
					"This user has already requested access to this insight with the given permission level");
		}

		String userType = token.getProvider().toString();
		SecurityInsightUtils.setUserAccessRequest(userId, userType, projectId, requestComment, insightId,
				requestPermission, user);

		if (Utility.isNotificationDatabaseEnabled()) {
//			String priority = AccessPermissionEnum.isOwner(requestPermission) ? NotificationConstants.Priority.HIGH
//					: NotificationConstants.Priority.MEDIUM;
//			NotificationDbUtils.createNotification(user, userId, userType, projectId,
//					NotificationConstants.Type.USER_REQUEST, NotificationConstants.APP_CATALOG, priority, null,
//					permission);

			EmailUtility.sendInsightAccessRequestEmailNotification(user, projectId, insightId, permission,
					requestComment);
		}
		return NounMetadata.getSuccessNounMessage("Successfully requested the insight");
	}

}
