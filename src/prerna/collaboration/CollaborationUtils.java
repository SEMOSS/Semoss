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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.playground.PlaygroundUtils;
import prerna.util.Constants;

/**
 * Collaboration rooms are playground-style rooms under their own system project
 * id.
 */
public final class CollaborationUtils {

	public static final String COLLABORATION_PROJECT_ID = "SYSTEM__COLLABORATION";
	public static final String MODE_COLLABORATION = "collaboration";
	// Set on an assignee's room; links it to the request it answers.
	public static final String ROOM_OPTION_DELEGATION_ACTION_ID = "delegation_action_id";
	// Set on the room a scheduled routine's agent step runs in; nobody is watching it.
	public static final String ROOM_OPTION_ROUTINE = "collaboration_routine";
	// Where that routine's answer goes (email, teams, none); its prompt says so.
	public static final String ROOM_OPTION_ROUTINE_DELIVERY = "collaboration_routine_delivery";
	// Only the server sets these; client option writes cannot add, change, or drop
	// them.
	public static final List<String> SERVER_OWNED_ROOM_OPTIONS = List.of(ROOM_OPTION_DELEGATION_ACTION_ID,
			ROOM_OPTION_ROUTINE, ROOM_OPTION_ROUTINE_DELIVERY);
	// Set by Work on a thread's assistant room (threadId, contextRevision,
	// modelId) until rooms opened from a thread switched to ROOM_OPTION_SOURCE.
	public static final String ROOM_OPTION_WORK_THREAD = "workThread";
	// Set on a room opened from a thread (threadId, title, channel, file,
	// messages).
	public static final String ROOM_OPTION_SOURCE = "source";

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

	// a routine's run room sits outside the project, so it stays out of the chat list
	public static boolean isCollaborationRoom(Room room) {
		return room != null && (COLLABORATION_PROJECT_ID.equals(room.getProjectId()) || isRoutineRoom(room));
	}

	/**
	 * A routine's unattended run room: it gets the collaboration read tools, and its ask tools are refused
	 * rather than paused, since nobody is there to approve them.
	 */
	public static boolean isRoutineRoom(Room room) {
		if (room == null) {
			return false;
		}
		Map<String, Object> options = room.getOptionsMap();
		return options != null && Boolean.TRUE.equals(options.get(ROOM_OPTION_ROUTINE));
	}

	// a routine room's delivery; email when it was not set
	public static String routineDelivery(Room room) {
		Map<String, Object> options = room == null ? null : room.getOptionsMap();
		Object delivery = options == null ? null : options.get(ROOM_OPTION_ROUTINE_DELIVERY);
		return delivery instanceof String value && !value.isBlank() ? value : "email";
	}

	/**
	 * The owner's own assistant chat: a collaboration room started from the home
	 * page or opened from a thread. An assignee's delegation room and a routine's
	 * run room are not one.
	 */
	public static boolean isAssistantRoom(Room room) {
		if (!isCollaborationRoom(room) || isRoutineRoom(room)) {
			return false;
		}
		Map<String, Object> options = room.getOptionsMap();
		Object delegation = options == null ? null : options.get(ROOM_OPTION_DELEGATION_ACTION_ID);
		return delegation == null || String.valueOf(delegation).isBlank();
	}

	/**
	 * The thread an assistant room was opened from (its source, or an older
	 * room's workThread option); null for a chat with no thread and any other
	 * room.
	 */
	public static String threadIdOf(Room room) {
		if (!isAssistantRoom(room)) {
			return null;
		}
		return threadIdOf(room.getOptionsMap());
	}

	// the same, from an assistant room's stored options
	static String threadIdOf(Map<String, Object> options) {
		if (options == null) {
			return null;
		}
		for (String key : List.of(ROOM_OPTION_SOURCE, ROOM_OPTION_WORK_THREAD)) {
			if (options.get(key) instanceof Map<?, ?> link) {
				Object threadId = link.get("threadId");
				if (threadId != null && !String.valueOf(threadId).isBlank()) {
					return String.valueOf(threadId);
				}
			}
		}
		return null;
	}

	/**
	 * The agent behind a chat's assistant, the Collaboration Agent, as {id, name,
	 * modelId}; null when the user cannot view it or it is not seeded or disabled.
	 */
	public static Map<String, Object> threadAgent(User user) {
		String id = Constants.AGENT_COLLABORATION;
		if (user == null || !SecurityProjectUtils.userCanViewProject(user, id)) {
			return null;
		}
		Map<String, Object> row = ModelInferenceLogsUtils.getWorkspaceEntry(id);
		if (row == null || Boolean.FALSE.equals(row.get("is_active"))) {
			return null;
		}
		JSONObject config = ModelInferenceLogsUtils.getWorkspaceConfigJson(id);
		Map<String, Object> agent = new LinkedHashMap<>();
		agent.put("id", id);
		agent.put("name", row.get("name") == null ? "Assistant" : String.valueOf(row.get("name")));
		agent.put("modelId", config == null ? null : config.optString("model_id", null));
		return agent;
	}

}
