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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Closes a user's room with an agent, for the admin Agent Runs page: the room
 * and its subagents' rooms are closed, so no agent runs in them again, and
 * every run in them that has not finished is cancelled, even while it executes.
 * The user no longer sees the rooms in their conversations. Admins only, and
 * recorded in the audit trail.
 */
public class AdminCloseRoomReactor extends AbstractReactor {

	private static final String USER_ID_KEY = "userId";
	private static final String USER_TYPE_KEY = "userType";

	public AdminCloseRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), USER_ID_KEY, USER_TYPE_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
		String userId = StringUtils.trimToNull(this.keyValue.get(USER_ID_KEY));
		String userType = StringUtils.trimToNull(this.keyValue.get(USER_TYPE_KEY));

		AgentRunService.RoomClosure closure = AgentRunService.get().closeRoomForAdmin(this.insight.getUser(), roomId,
				userId, userType);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("roomId", roomId);
		result.put("closedRoomIds", closure.closedRoomIds());
		result.put("cancelledRunIds", closure.cancelledRunIds());
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Admins only. Closes a user's room with an agent, and its subagents' rooms, so no agent runs in them again, cancelling every run in them that has not finished, even while it executes. The user no longer sees the rooms. Returns the rooms closed and the runs cancelled.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "The room to close.";
		} else if (USER_ID_KEY.equals(key)) {
			return "The user the room belongs to; rooms are kept per user.";
		} else if (USER_TYPE_KEY.equals(key)) {
			return "The login type of the user, such as NATIVE. Runs that store no login type match the user id alone.";
		}
		return super.getDescriptionForKey(key);
	}
}
