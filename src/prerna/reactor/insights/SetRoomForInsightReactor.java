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

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetRoomForInsightReactor extends AbstractReactor {

	public SetRoomForInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String userId = user.getPrimaryLoginToken().getId();

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId == null || roomId.isEmpty()) {
			throw new IllegalArgumentException("Room ID is required");
		}

		boolean isOwner = !ModelInferenceLogsUtils.getUserActiveRooms(roomId, userId).isEmpty();
		if (!isOwner) {
			throw new IllegalArgumentException("User is not an owner of an active room");
		}

		List<Map<String, Object>> activeRooms = ModelInferenceLogsUtils.getUserActiveRooms(roomId, userId);
		if (activeRooms == null || activeRooms.isEmpty()) {
			throw new IllegalArgumentException("User is not the owner of the room or room is not active");
		}

		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		if (room == null) {
			throw new IllegalArgumentException("Room not found");
		}

		this.insight.setRoomForInsight(room);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "Set the room for the insight. File system operations on the insight will now alter/show the room files";
	}

}