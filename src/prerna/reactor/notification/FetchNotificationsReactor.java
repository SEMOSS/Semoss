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
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Fetches the user's notifications, newest first. By default every visible
 * notification in the scope is marked read, which is how the platform bell
 * clears; pass markRead=false to leave read state to MarkNotificationRead.
 *
 * <pre>{@code
 * FetchNotifications(limit=["20"], offset=["0"]);
 * FetchNotifications(scopeType=["APP"], scopeId=["SYSTEM__COLLABORATION"], markRead=[false]);
 * }</pre>
 */
public class FetchNotificationsReactor extends AbstractReactor {
	private static final String SCOPE_TYPE = "scopeType";
	private static final String SCOPE_ID = "scopeId";
	private static final String MARK_READ = "markRead";

	public FetchNotificationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), SCOPE_TYPE,
				SCOPE_ID, MARK_READ };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		if (!Utility.isNotificationDatabaseEnabled()) {
			throw new IllegalArgumentException("Notifications are not enabled on this instance");
		}
		organizeKeys();
		User user = requireUser();
		String limit = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offset = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		String scopeId = this.keyValue.get(SCOPE_ID);
		String markRead = this.keyValue.get(MARK_READ);
		String scopeType = NotificationDbUtils.resolveReadScope(user, this.keyValue.get(SCOPE_TYPE), scopeId);

		List<Map<String, Object>> allNotifications = NotificationDbUtils.fetchNotifications(user, scopeType, scopeId,
				limit, offset);
		boolean resetReadState = markRead == null || markRead.isBlank() || Boolean.parseBoolean(markRead.trim());
		if (resetReadState && !allNotifications.isEmpty()) {
			NotificationDbUtils.resetNotificationActionType(user, scopeType, scopeId);
		}

		return new NounMetadata(allNotifications, PixelDataType.MAP);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Fetch all user notifications")
				.key(SCOPE_TYPE, "ALL (default), SYSTEM, or APP.")
				.key(SCOPE_ID,
						"The app id when scopeType is APP, e.g. SYSTEM__COLLABORATION for the Collaboration inbox.")
				.key(MARK_READ,
						"Whether fetching marks every visible notification in the scope as read. Defaults to true.");
	}
}
