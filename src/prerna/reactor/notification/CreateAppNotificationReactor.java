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

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.notifications.NotificationService;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/** Creates an app-wide announcement for the authenticated app editor/owner. */
public class CreateAppNotificationReactor extends AbstractReactor {

	private static final String TITLE = "title";
	private static final String PRIORITY = "priority";

	public CreateAppNotificationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), TITLE, ReactorKeysEnum.MESSAGE.getKey(),
				PRIORITY };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
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

		String notificationId = NotificationService.createAppAnnouncement(user,
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()), this.keyValue.get(TITLE),
				this.keyValue.get(ReactorKeysEnum.MESSAGE.getKey()), this.keyValue.get(PRIORITY));
		Map<String, Object> response = new HashMap<>();
		response.put("notificationId", notificationId);
		response.put("scopeType", "APP");
		response.put("audienceType", "APP_MEMBERS");
		return new NounMetadata(response, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Create an announcement for all users of an app";
	}
}
