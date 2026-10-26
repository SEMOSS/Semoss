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
package prerna.reactor.engine;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EmailUtility;
import prerna.util.NotificationConstants;
import prerna.util.Utility;

public class RequestEngineReactor extends AbstractReactor {

	public RequestEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PERMISSION.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		String permission = this.keyValue.get(this.keysToGet[1]);
		String requestComment = this.keyValue.get(this.keysToGet[2]);
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to request an engine",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// turn permission into an integer in case it was added as the string version of
		// the value
		int requestPermission = -1;
		try {
			requestPermission = Integer.parseInt(permission);
		} catch (NumberFormatException ignore) {
			requestPermission = AccessPermissionEnum.getPermissionByValue(permission).getId();
		}

		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String userId = token.getId();
		// check user permission for the engine
		Integer currentUserPermission = SecurityEngineUtils.getUserEnginePermission(userId, engineId);
		if (currentUserPermission != null && requestPermission == currentUserPermission) {
			throw new IllegalArgumentException(
					"This user already has access to this engine with the given permission level");
		}
		// check user pending permission for engine
		Integer currentPendingUserPermission = SecurityEngineUtils.getUserAccessRequestEnginePermission(userId,
				engineId);
		if (currentPendingUserPermission != null && requestPermission == currentPendingUserPermission) {
			throw new IllegalArgumentException(
					"This user has already requested access to this engine with the given permission level");
		}
		// checking to make sure you can request access
		boolean canRequest = SecurityEngineUtils.engineIsDiscoverable(engineId)
				|| SecurityEngineUtils.userHasExplicitAccess(user, engineId);
		String engineType = String.valueOf(SecurityEngineUtils.getEngineType(engineId)).toLowerCase();
		if (canRequest) {
			String userType = token.getProvider().toString();
			SecurityEngineUtils.setUserAccessRequest(userId, userType, engineId, requestComment, requestPermission,
					user);

			if (Utility.isNotificationDatabaseEnabled()) {
				String priority = AccessPermissionEnum.isOwner(requestPermission) ? NotificationConstants.Priority.HIGH
						: NotificationConstants.Priority.MEDIUM;
					NotificationDbUtils.createNotification(user, userId, userType, engineId,
							NotificationConstants.Type.USER_REQUEST, engineType, priority, null, permission,
							NotificationConstants.DisplaySurface.BELL);

				EmailUtility.sendAccessRequestEmailNotification(user, engineId, permission, requestComment,
						EmailUtility.RESOURCE_TYPE.ENGINE);
			}
			return NounMetadata.getSuccessNounMessage("Successfully requested access to engine '" + engineId + "'");
		}

		return NounMetadata.getErrorNounMessage("Engine '" + engineId + "' is not requestable");
	}

}
