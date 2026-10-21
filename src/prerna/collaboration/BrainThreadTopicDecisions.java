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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Minimal, owner-scoped negative relationships. No email content or Work state is retained here. */
public final class BrainThreadTopicDecisions {
	private BrainThreadTopicDecisions() {
	}

	static void lockThread(String ownerId, String ownerType, String threadId) {
		String found = CollaborationDbUtils.queryOne("SELECT THREAD_ID FROM BRAIN_THREAD WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID = ? FOR UPDATE", rs -> rs.getString(1), ownerId, ownerType, threadId);
		if (found == null) {
			throw new IllegalArgumentException("Thread not found");
		}
	}

	static Set<String> rejected(String ownerId, String ownerType, String threadId) {
		return new HashSet<>(CollaborationDbUtils.query("SELECT TOPIC_ID FROM BRAIN_THREAD_TOPIC_REJECTION "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> rs.getString(1),
				ownerId, ownerType, threadId));
	}

	/** Called under the thread row lock in the same transaction as its links. */
	static void remember(String ownerId, String ownerType, String threadId, String topicId, boolean rejected) {
		CollaborationDbUtils.update("DELETE FROM BRAIN_THREAD_TOPIC_REJECTION WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID = ? AND TOPIC_ID = ?", ownerId, ownerType, threadId, topicId);
		if (rejected) {
			CollaborationDbUtils.update("INSERT INTO BRAIN_THREAD_TOPIC_REJECTION "
					+ "(OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, CHANGED_BY, CHANGED_AT) VALUES (?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, threadId, topicId, BrainProfileUtils.YOU, CollaborationDbUtils.now());
		}
	}

	/** A combined scope keeps negative examples unless the conversation already belongs in either scope. */
	static void merge(Connection conn, String ownerId, String ownerType, String sourceId, String targetId)
			throws SQLException {
		List<String> negatives = CollaborationDbUtils.query("SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC_REJECTION "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? ORDER BY THREAD_ID", rs -> rs.getString(1),
				ownerId, ownerType, sourceId);
		for (String threadId : negatives) {
			if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID = ? AND TOPIC_ID IN (?, ?)", ownerId, ownerType, threadId, sourceId, targetId)) {
				remember(ownerId, ownerType, threadId, targetId, true);
			}
		}
		CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_THREAD_TOPIC_REJECTION WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND TOPIC_ID = ? AND THREAD_ID IN (SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID IN (?, ?))", ownerId, ownerType, targetId,
				ownerId, ownerType, sourceId, targetId);
		CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_THREAD_TOPIC_REJECTION WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND TOPIC_ID = ?", ownerId, ownerType, sourceId);
	}
}
