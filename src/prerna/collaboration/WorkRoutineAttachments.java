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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.javatuples.Pair;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.run.AutomationRunStore;

/**
 * Files a custom routine's agent leaves for its owner: whatever it saves in the attachments folder of its working
 * folder goes with the emailed result. The delivery step stages them into its own insight, where the send-mail
 * reactors read attachments from.
 */
public final class WorkRoutineAttachments {
	// the folder in the routine room the agent saves files to
	public static final String FOLDER = "attachments";
	// where they are copied in the delivery step's insight
	static final String STAGED = "routine-attachments";

	private WorkRoutineAttachments() { }

	/**
	 * Copies the run's files into the caller's insight folder.
	 *
	 * @return attachments, paths relative to the insight folder, and skipped, the names that would not fit in the
	 *         email under the mail helper's message limit
	 */
	public static Map<String, Object> stage(Insight insight, String runId) throws IOException {
		if (runId == null || runId.isBlank()) {
			throw new IllegalArgumentException("runId is required");
		}
		// only the owner's own routine, while it runs
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(insight.getUser());
		boolean mine = CollaborationDbUtils.exists("SELECT 1 FROM " + WorkCustomRoutine.KIND.table()
				+ " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND RUN_ID = ? AND RUN_STATUS = 'running'",
				owner.getValue0(), owner.getValue1(), runId.trim());
		if (!mine) {
			throw new IllegalArgumentException("That is not one of your running routines");
		}
		List<String> attachments = new ArrayList<>();
		List<String> skipped = new ArrayList<>();
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("attachments", attachments);
		result.put("skipped", skipped);

		String roomId = agentRoomId(runId.trim());
		if (roomId == null) {
			return result;
		}
		// loads the room for this user only
		Room room = RoomUtils.getOrLoadRoom(roomId, insight);
		if (!CollaborationUtils.isRoutineRoom(room) || room.getRoomFolderPath() == null) {
			return result;
		}
		Path folder = Path.of(room.getRoomFolderPath(), FOLDER);
		if (!Files.isDirectory(folder, LinkOption.NOFOLLOW_LINKS)) {
			return result;
		}
		List<Path> files;
		try (Stream<Path> listed = Files.list(folder)) {
			files = listed.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
					.filter(p -> !p.getFileName().toString().startsWith(".")).sorted().toList();
		}
		Path target = Path.of(insight.getInsightFolder(), STAGED);
		// the one message limit lives in the mail helper
		long fits = MicrosoftOutlookMailHelper.maxAttachmentBytes();
		long total = 0;
		for (Path file : files) {
			String name = file.getFileName().toString();
			long size = Files.size(file);
			if (total + size > fits) {
				skipped.add(name);
				continue;
			}
			Files.createDirectories(target);
			Files.copy(file, target.resolve(name), StandardCopyOption.REPLACE_EXISTING);
			total += size;
			attachments.add(STAGED + "/" + name);
		}
		return result;
	}

	// the room the routine's agent step ran in
	private static String agentRoomId(String runId) {
		String roomId = null;
		for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
			if (WorkCustomRoutine.KIND.outputNode().equals(node.get(AutomationConstants.NODE_ID))
					&& node.get(AutomationConstants.PARENT_NODE_ID) == null
					&& node.get(AutomationConstants.ROOM_ID) instanceof String id && !id.isBlank()) {
				roomId = id;
			}
		}
		return roomId;
	}
}
