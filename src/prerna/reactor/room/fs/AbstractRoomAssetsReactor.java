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
package prerna.reactor.room.fs;

import java.io.File;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Base for the reactors that work on the files in a room's folder, named by its
 * room id. Only the room's owner reaches the folder, and only while the room is
 * active.
 */
public abstract class AbstractRoomAssetsReactor extends AbstractReactor {

	private static final String ROOM_ID_DESCRIPTION = """
			The id of the room whose folder holds the files. The signed in user must own the room, and the room \
			must be active.""";

	/**
	 * The id of the room named by the roomId key.
	 *
	 * @return the trimmed room id
	 * @throws IllegalArgumentException if no room id is passed
	 */
	protected String getRoomId() {
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass the roomId of the room");
		}
		return roomId.trim();
	}

	/**
	 * The folder of the room named by the roomId key, once the signed in user is
	 * known to own the room and the room is active. A room not loaded in this
	 * session may have been written on another server, so its folder is pulled
	 * from cloud storage first.
	 *
	 * @param user the signed in user
	 * @return the room's folder, created when missing
	 * @throws IllegalArgumentException if the room does not exist, is not active,
	 *                                  or belongs to someone else
	 */
	protected String getRoomFolder(User user) {
		String roomId = getRoomId();
		if (!ownsActiveRoom(user, roomId)) {
			throw new IllegalArgumentException(
					"Room " + roomId + " does not exist, is not active, or the user is not its owner");
		}
		if (!user.getRoomHash().containsKey(roomId)) {
			ClusterUtil.pullRoom(roomId);
		}
		String roomFolder = Room.roomFolderPath(roomId);
		new File(roomFolder).mkdirs();
		return roomFolder;
	}

	/**
	 * Uploads the room's folder to cloud storage after a write, without waiting
	 * for the upload. Does nothing outside a cluster.
	 */
	protected void pushRoom() {
		ClusterUtil.pushRoomAsync(getRoomId());
	}

	/**
	 * Whether any of the user's logins owns the room and the room is active.
	 *
	 * @param user   the signed in user
	 * @param roomId the room id
	 * @return true when one of the user's logins owns the active room
	 */
	private static boolean ownsActiveRoom(User user, String roomId) {
		// ROOM stores the owner's id without a login type, so a login with that id owns it
		for (Pair<String, String> login : User.getUserIdAndType(user)) {
			if (!ModelInferenceLogsUtils.getUserActiveRooms(roomId, login.getValue0()).isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().key(ReactorKeysEnum.ROOM_ID, ROOM_ID_DESCRIPTION);
	}

}
