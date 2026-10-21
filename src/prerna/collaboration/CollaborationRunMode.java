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

import prerna.engine.impl.model.Room;

/**
 * What a Collaboration run may do, decided once from its room. Only the server
 * sets the room options this reads (CollaborationUtils.SERVER_OWNED_ROOM_OPTIONS).
 */
public enum CollaborationRunMode {

	// the owner's own chat: every tool; ask tools pause for the owner
	CHAT,
	// a routine's unattended run: the chat minus anything that needs a person, so
	// auto tools only; its delivery step sends results to the owner alone
	ROUTINE;

	/** The mode of a run in this room; null for a room outside Collaboration and for a delegation room. */
	public static CollaborationRunMode of(Room room) {
		if (CollaborationUtils.isRoutineRoom(room)) {
			return ROUTINE;
		}
		if (CollaborationUtils.isAssistantRoom(room)) {
			return CHAT;
		}
		return null;
	}

	public static boolean isRoutine(Room room) {
		return of(room) == ROUTINE;
	}

	public static boolean isChat(Room room) {
		return of(room) == CHAT;
	}

	/** Whether ask tools can wait for the owner; a routine leaves them out and refuses them. */
	public boolean ownerPresent() {
		return this == CHAT;
	}

	/** Tools about the chat itself (TagTopic, CompleteGoal), the thread brief and the after-run memory review. */
	public boolean chatOnly() {
		return this == CHAT;
	}
}
