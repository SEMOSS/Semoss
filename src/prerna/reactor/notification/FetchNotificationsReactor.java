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
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class FetchNotificationsReactor extends AbstractReactor {

	public FetchNotificationsReactor() {
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
		String limit = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offset = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
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

		List<Map<String, Object>> allNotifications = null;
		allNotifications = NotificationDbUtils.fetchAllNotifications(user, limit, offset);
		if (!allNotifications.isEmpty()) {
			NotificationDbUtils.resetNotificationActionType(user);
		}

		return new NounMetadata(allNotifications, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Fetch all user notifications";
	}
}
