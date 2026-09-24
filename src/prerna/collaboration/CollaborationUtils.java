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
package prerna.collaboration;

import java.util.List;

import prerna.engine.impl.model.Room;
import prerna.playground.PlaygroundUtils;

/**
 * Collaboration rooms are playground-style rooms under their own system project
 * id.
 */
public final class CollaborationUtils {

	public static final String COLLABORATION_PROJECT_ID = "SYSTEM__COLLABORATION";
	public static final String MODE_COLLABORATION = "collaboration";
	// Set on an assignee's room; links it to the request it answers.
	public static final String ROOM_OPTION_DELEGATION_ACTION_ID = "delegation_action_id";
	// Only the server sets these; client option writes cannot add, change, or drop
	// them.
	public static final List<String> SERVER_OWNED_ROOM_OPTIONS = List.of(ROOM_OPTION_DELEGATION_ACTION_ID);

	private CollaborationUtils() {
	}

	/**
	 * System project id for a playground room mode; null or blank is a normal
	 * playground room.
	 */
	public static String projectIdForMode(String mode) {
		if (mode == null || mode.isBlank()) {
			return PlaygroundUtils.PLAYGROUND_PROJECT_ID;
		}
		if (!MODE_COLLABORATION.equals(mode.trim().toLowerCase())) {
			throw new IllegalArgumentException("Unknown room mode '" + mode + "'. Supported: " + MODE_COLLABORATION);
		}
		return COLLABORATION_PROJECT_ID;
	}

	public static boolean isCollaborationRoom(Room room) {
		return room != null && COLLABORATION_PROJECT_ID.equals(room.getProjectId());
	}
}
