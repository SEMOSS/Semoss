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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.om.Insight;

/** Keeps mail sent from a collaboration chat in that chat and its linked topics. */
public final class BrainOutgoingEmail {

	private BrainOutgoingEmail() {
	}

	/** Commit the relationship before sending; a tracking failure must leave the email unsent. */
	public static void trackDraft(Insight insight, String draftId) throws Exception {
		if (insight == null || insight.getRoomId() == null || insight.getRoomId().isBlank()) {
			return;
		}
		Room room = RoomUtils.getOrLoadRoom(insight.getRoomId(), insight);
		if (!CollaborationUtils.isAssistantRoom(room)) {
			return;
		}
		User user = insight.getUser();
		// Resolve by id to verify ownership, including rooms loaded from the shared room cache.
		List<String> topics = BrainTopicRoomUtils.topicsOf(user, room.getId());
		Map<String, Object> draft = new MicrosoftOutlookMailHelper()
				.getMessage(MicrosoftLoginUtils.getValidAccessToken(user), null, draftId);
		if (draft == null || !draftId.equals(draft.get("id"))
				|| !(draft.get("conversationId") instanceof String conversationId) || conversationId.isBlank()) {
			throw new IllegalStateException("The email's conversation could not be tracked. It has not been sent.");
		}
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String threadKey = "email:" + conversationId;
		synchronized (CollaborationDbUtils.ownerLock(BrainSync.INGEST_LOCK, ownerId, ownerType)) {
			CollaborationDbUtils.batch(conn -> {
				String threadId = CollaborationDbUtils.queryOne("SELECT THREAD_ID FROM BRAIN_THREAD WHERE "
						+ "OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_KEY = ?", rs -> rs.getString(1),
						ownerId, ownerType, threadKey);
				if (threadId == null) {
					threadId = CollaborationDbUtils.queryOne("SELECT THREAD_ID FROM BRAIN_THREAD_ALIAS WHERE "
							+ "OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_KEY = ?", rs -> rs.getString(1),
							ownerId, ownerType, threadKey);
				}
				if (threadId == null) {
					threadId = "th-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "thread", threadKey);
					Timestamp now = CollaborationDbUtils.now();
					CollaborationDbUtils.update(conn,
							"INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, SOURCE, "
									+ "SUBJECT, MUTED, AUTOMATED, MESSAGE_COUNT, LAST_MESSAGE_AT, CREATED_AT) "
									+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
							ownerId, ownerType, threadId, threadKey, "email",
							BrainMailImport.cleanSubject((String) draft.get("subject")), false, false, 0, now, now);
				}
				// Outlook may file an outside client's reply under a new conversation before importing Sent Items.
				// shortcut: missing draft Message-ID needs sent-first ingestion; add late-reply reconciliation if observed.
				if (draft.get("internetMessageId") instanceof String messageId && !messageId.isBlank()) {
					String messageKey = "message:email:"
							+ CollaborationDbUtils.deterministicId(ownerId, ownerType, "email", messageId);
					if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_ALIAS WHERE OWNER_ID = ? "
							+ "AND OWNER_TYPE = ? AND THREAD_KEY = ?", ownerId, ownerType, messageKey)) {
						CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_THREAD_ALIAS (OWNER_ID, OWNER_TYPE, "
								+ "THREAD_KEY, THREAD_ID, CREATED_AT) VALUES (?, ?, ?, ?, ?)",
								ownerId, ownerType, messageKey, threadId, CollaborationDbUtils.now());
					}
				}
				for (String topicId : topics) {
					BrainThreadUtils.linkThreadTopic(user, threadId, topicId, false, false);
				}
				BrainThreadRoomUtils.linkRoomThread(user, room.getId(), threadId, false);
			});
		}
	}
}
