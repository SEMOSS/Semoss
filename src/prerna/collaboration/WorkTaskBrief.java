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
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.engine.impl.model.Room;

/** The work item a chat was opened for, as of this turn, for the runtime note. */
public final class WorkTaskBrief {

	private static final Logger classLogger = LogManager.getLogger(WorkTaskBrief.class);
	private static final int MAX_UPDATES = 5;

	private WorkTaskBrief() {
	}

	/** The work-item section, or null when the room was not opened for one. Never throws. */
	@SuppressWarnings("unchecked")
	public static String runtimeNote(User user, Room room) {
		if (user == null || room == null || !CollaborationUtils.isAssistantRoom(room)) {
			return null;
		}
		try {
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			String taskId = WorkTaskUtils.taskForRoom(owner.getValue0(), owner.getValue1(), room.getId());
			if (taskId == null) {
				return null;
			}
			Map<String, Object> task = WorkTaskUtils.getWork(user, taskId);
			StringBuilder out = new StringBuilder("[Work item]\nThis chat is for working the owner's work item "
					+ "below, as of this turn. Help move it forward; use GetWork for more and the step tools to add, "
					+ "finish or reword its steps.\n");
			out.append("Item: ").append(task.get("title")).append(" (id ").append(taskId).append(", ")
					.append(task.get("status")).append(")");
			for (Map<String, Object> t : (List<Map<String, Object>>) task.get("topics")) {
				out.append("\nTopic: ").append(t.get("name"));
			}
			out.append("\nSteps:");
			for (Map<String, Object> s : (List<Map<String, Object>>) task.get("steps")) {
				String status = String.valueOf(s.get("status"));
				if ("dropped".equals(status)) {
					continue;
				}
				Object who = s.get("assigneeName");
				out.append("\n- [").append(status).append("] ").append(s.get("text"))
						.append(who == null ? "" : " (" + who + ")").append(" (step ").append(s.get("id")).append(")");
			}
			List<Map<String, Object>> threads = (List<Map<String, Object>>) task.get("threads");
			if (!threads.isEmpty()) {
				out.append("\nFrom:");
				for (Map<String, Object> t : threads) {
					out.append("\n- ").append(t.get("subject")).append(" (").append(t.get("source")).append(", thread ")
							.append(t.get("threadId")).append(")");
				}
			}
			List<Map<String, Object>> timeline = (List<Map<String, Object>>) task.get("timeline");
			if (!timeline.isEmpty()) {
				out.append("\nRecent updates:");
				for (Map<String, Object> u : timeline.subList(0, Math.min(MAX_UPDATES, timeline.size()))) {
					out.append("\n- ").append(u.get("text"));
				}
			}
			return out.append("\n[/Work item]").toString();
		} catch (RuntimeException e) {
			classLogger.warn("Could not build the work item brief for room {}", room.getId(), e);
			return null;
		}
	}
}
