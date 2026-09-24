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

import java.sql.Timestamp;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Marks one notification read, or every unread notification in a scope when no
 * notificationId is given.
 *
 * <pre>{@code
 * MarkNotificationRead(notificationId=["<notificationId>"]);
 * MarkNotificationRead(scopeType=["APP"], scopeId=["SYSTEM__COLLABORATION"]);
 * }</pre>
 */
public class MarkNotificationReadReactor extends AbstractReactor {

	private static final String SCOPE_TYPE = "scopeType";
	private static final String SCOPE_ID = "scopeId";

	public MarkNotificationReadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NOTIFICATION_ID.getKey(), SCOPE_TYPE, SCOPE_ID };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		if (!Utility.isNotificationDatabaseEnabled()) {
			throw new IllegalArgumentException("Notifications are not enabled on this instance");
		}
		User user = this.insight.getUser();
		if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
			throwAnonymousUserError();
		}

		organizeKeys();
		String notificationId = StringUtils.trimToNull(this.keyValue.get(this.keysToGet[0]));
		if (notificationId != null) {
			Timestamp readAt = Utility.getCurrentSqlTimestampUTC();
			NotificationDbUtils.markNotificationRead(user, notificationId, readAt);
		} else {
			String scopeId = this.keyValue.get(SCOPE_ID);
			String scopeType = NotificationDbUtils.resolveReadScope(user, this.keyValue.get(SCOPE_TYPE), scopeId);
			NotificationDbUtils.markAllNotificationsRead(user, scopeType, scopeId);
		}
		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Marks a notification as read by the user, or every notification in a scope when no id is given";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SCOPE_TYPE.equals(key)) {
			return "Scope to mark read when no notificationId is given: ALL (default), SYSTEM, or APP.";
		}
		if (SCOPE_ID.equals(key)) {
			return "The app id when scopeType is APP, e.g. SYSTEM__COLLABORATION for the Collaboration inbox.";
		}
		return super.getDescriptionForKey(key);
	}

}
