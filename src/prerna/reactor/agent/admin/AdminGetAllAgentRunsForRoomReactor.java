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
package prerna.reactor.agent.admin;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns every run a user has in a room, newest first, for the admin Agent
 * Runs page: the run list the room's owner sees on the agent's Activity tab.
 * Admins only.
 */
public class AdminGetAllAgentRunsForRoomReactor extends AbstractReactor {

	private static final String USER_ID_KEY = "userId";
	private static final String USER_TYPE_KEY = "userType";

	public AdminGetAllAgentRunsForRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), USER_ID_KEY, USER_TYPE_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		AdminAgentRunReadService readService = AdminAgentRunReadService.getInstance(this.insight.getUser());
		organizeKeys();
		String roomId = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
		String userId = StringUtils.trimToNull(this.keyValue.get(USER_ID_KEY));
		String userType = StringUtils.trimToNull(this.keyValue.get(USER_TYPE_KEY));
		return new NounMetadata(readService.getRunsForRoom(roomId, userId, userType), PixelDataType.VECTOR,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Admins only. Returns every run a user has in a room, newest first, each with its model's name.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "The room whose runs are returned.";
		} else if (USER_ID_KEY.equals(key)) {
			return "The user the room belongs to; rooms are kept per user.";
		} else if (USER_TYPE_KEY.equals(key)) {
			return "The login type of the user, such as NATIVE. Runs that store no login type match the user id alone.";
		}
		return super.getDescriptionForKey(key);
	}
}
