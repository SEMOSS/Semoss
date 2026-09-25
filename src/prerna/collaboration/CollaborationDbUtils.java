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
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.owl.AbstractOwlCreator.OwlIndex;
import prerna.util.SystemEngineRegistry;

// Loads the Collaboration database: schema sync and indexes, like NotificationDbUtils
public class CollaborationDbUtils {

	static boolean initialized = false;

	private CollaborationDbUtils() {

	}

	public static void loadCollaborationDatabase() throws Exception {
		IRDBMSEngine collaborationDb = SystemEngineRegistry.getCollaborationDb();
		CollaborationOwlCreator owlCreator = new CollaborationOwlCreator(collaborationDb.getQueryUtil());
		if (owlCreator.needsRemake(collaborationDb)) {
			owlCreator.remakeOwl(collaborationDb);
		}
		initialize(owlCreator.getDBSchema());
		initialized = true;
	}

	/**
	 * Determine if the collaboration db is present.
	 *
	 * @return
	 */
	public static boolean isInitalized() {
		return CollaborationDbUtils.initialized;
	}

	private static void initialize(List<Pair<String, List<Pair<String, String>>>> dbSchema) throws Exception {
		IRDBMSEngine collaborationDb = SystemEngineRegistry.getCollaborationDb();
		Connection conn = collaborationDb.getConnection();
		try {
			// create the tables and columns from the OWL creator schema
			AbstractOwlCreator.syncSchema(collaborationDb, conn, dbSchema);

			AbstractOwlCreator.syncIndexes(collaborationDb, conn, List.of(
					// owner and sources
					OwlIndex.of("COLLAB_OWNER_OWNER_INDEX", "COLLAB_OWNER", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("COLLAB_OWNER_MS_USER_INDEX", "COLLAB_OWNER", "MS_USER_ID"),
					OwlIndex.of("SOURCE_CONNECTION_OWNER_INDEX", "SOURCE_CONNECTION", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("SOURCE_CONNECTION_SOURCE_INDEX", "SOURCE_CONNECTION", "OWNER_ID", "OWNER_TYPE",
							"SOURCE"),
					OwlIndex.of("SOURCE_SYNC_STATE_OWNER_INDEX", "SOURCE_SYNC_STATE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("SOURCE_SYNC_STATE_SOURCE_FOLDER_INDEX", "SOURCE_SYNC_STATE", "OWNER_ID", "OWNER_TYPE",
							"SOURCE", "FOLDER"),
					OwlIndex.of("INBOUND_EVENT_OWNER_INDEX", "INBOUND_EVENT", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("INBOUND_EVENT_EVENT_KEY_INDEX", "INBOUND_EVENT", "EVENT_KEY"),

					// brain: you
					OwlIndex.of("BRAIN_PROFILE_OWNER_INDEX", "BRAIN_PROFILE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_SETTINGS_OWNER_INDEX", "BRAIN_SETTINGS", "OWNER_ID", "OWNER_TYPE"),

					// brain: people
					OwlIndex.of("BRAIN_ACCOUNT_OWNER_INDEX", "BRAIN_ACCOUNT", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_ACCOUNT_ACCOUNT_ID_INDEX", "BRAIN_ACCOUNT", "OWNER_ID", "OWNER_TYPE",
							"ACCOUNT_ID"),
					OwlIndex.of("BRAIN_PERSON_OWNER_INDEX", "BRAIN_PERSON", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_PERSON_PERSON_ID_INDEX", "BRAIN_PERSON", "OWNER_ID", "OWNER_TYPE",
							"PERSON_ID"),
					OwlIndex.of("BRAIN_PERSON_EMAIL_NORM_INDEX", "BRAIN_PERSON", "OWNER_ID", "OWNER_TYPE",
							"EMAIL_NORM"),
					OwlIndex.of("BRAIN_PERSON_ADDRESS_OWNER_INDEX", "BRAIN_PERSON_ADDRESS", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_PERSON_ADDRESS_PERSON_ID_INDEX", "BRAIN_PERSON_ADDRESS", "OWNER_ID",
							"OWNER_TYPE", "PERSON_ID"),

					// brain: topics
					OwlIndex.of("BRAIN_TOPIC_OWNER_INDEX", "BRAIN_TOPIC", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_TOPIC_TOPIC_ID_INDEX", "BRAIN_TOPIC", "OWNER_ID", "OWNER_TYPE", "TOPIC_ID"),
					OwlIndex.of("BRAIN_TOPIC_NOTE_OWNER_INDEX", "BRAIN_TOPIC_NOTE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_TOPIC_NOTE_TOPIC_ID_INDEX", "BRAIN_TOPIC_NOTE", "OWNER_ID", "OWNER_TYPE",
							"TOPIC_ID"),
					OwlIndex.of("BRAIN_TOPIC_PERSON_OWNER_INDEX", "BRAIN_TOPIC_PERSON", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_TOPIC_PERSON_TOPIC_ID_INDEX", "BRAIN_TOPIC_PERSON", "OWNER_ID", "OWNER_TYPE",
							"TOPIC_ID"),
					OwlIndex.of("BRAIN_TOPIC_PERSON_PERSON_ID_INDEX", "BRAIN_TOPIC_PERSON", "OWNER_ID", "OWNER_TYPE",
							"PERSON_ID"),

					// brain: threads
					OwlIndex.of("BRAIN_THREAD_OWNER_INDEX", "BRAIN_THREAD", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_THREAD_THREAD_ID_INDEX", "BRAIN_THREAD", "OWNER_ID", "OWNER_TYPE",
							"THREAD_ID"),
					OwlIndex.of("BRAIN_THREAD_THREAD_KEY_INDEX", "BRAIN_THREAD", "OWNER_ID", "OWNER_TYPE",
							"THREAD_KEY"),
					OwlIndex.of("BRAIN_THREAD_TOPIC_OWNER_INDEX", "BRAIN_THREAD_TOPIC", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_THREAD_TOPIC_THREAD_ID_INDEX", "BRAIN_THREAD_TOPIC", "OWNER_ID", "OWNER_TYPE",
							"THREAD_ID"),
					OwlIndex.of("BRAIN_THREAD_TOPIC_TOPIC_ID_INDEX", "BRAIN_THREAD_TOPIC", "OWNER_ID", "OWNER_TYPE",
							"TOPIC_ID"),
					OwlIndex.of("BRAIN_THREAD_PARTICIPANT_OWNER_INDEX", "BRAIN_THREAD_PARTICIPANT", "OWNER_ID",
							"OWNER_TYPE"),
					OwlIndex.of("BRAIN_THREAD_PARTICIPANT_THREAD_ID_INDEX", "BRAIN_THREAD_PARTICIPANT", "OWNER_ID",
							"OWNER_TYPE", "THREAD_ID"),
					OwlIndex.of("BRAIN_THREAD_PARTICIPANT_PERSON_ID_INDEX", "BRAIN_THREAD_PARTICIPANT", "OWNER_ID",
							"OWNER_TYPE", "PERSON_ID"),
					OwlIndex.of("BRAIN_THREAD_LINK_OWNER_INDEX", "BRAIN_THREAD_LINK", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_THREAD_LINK_FROM_THREAD_KEY_INDEX", "BRAIN_THREAD_LINK", "OWNER_ID",
							"OWNER_TYPE", "FROM_THREAD_KEY"),
					OwlIndex.of("BRAIN_THREAD_LINK_TO_THREAD_KEY_INDEX", "BRAIN_THREAD_LINK", "OWNER_ID",
							"OWNER_TYPE", "TO_THREAD_KEY"),
					OwlIndex.of("BRAIN_MESSAGE_OWNER_INDEX", "BRAIN_MESSAGE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_MESSAGE_MESSAGE_KEY_INDEX", "BRAIN_MESSAGE", "OWNER_ID", "OWNER_TYPE",
							"MESSAGE_KEY"),
					OwlIndex.of("BRAIN_MESSAGE_THREAD_ID_INDEX", "BRAIN_MESSAGE", "OWNER_ID", "OWNER_TYPE",
							"THREAD_ID"),

					// brain: control
					OwlIndex.of("BRAIN_RULE_OWNER_INDEX", "BRAIN_RULE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_RULE_RULE_ID_INDEX", "BRAIN_RULE", "OWNER_ID", "OWNER_TYPE", "RULE_ID"),
					OwlIndex.of("BRAIN_REVIEW_OWNER_INDEX", "BRAIN_REVIEW", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_REVIEW_REVIEW_ID_INDEX", "BRAIN_REVIEW", "OWNER_ID", "OWNER_TYPE",
							"REVIEW_ID"),
					OwlIndex.of("BRAIN_CHANGE_OWNER_INDEX", "BRAIN_CHANGE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_CHANGE_ENTITY_INDEX", "BRAIN_CHANGE", "OWNER_ID", "OWNER_TYPE", "ENTITY_TYPE",
							"ENTITY_ID"),
					OwlIndex.of("BRAIN_EXCLUSION_FINGERPRINT_OWNER_INDEX", "BRAIN_EXCLUSION_FINGERPRINT", "OWNER_ID",
							"OWNER_TYPE"),
					OwlIndex.of("BRAIN_EXCLUSION_FINGERPRINT_THREAD_PERSON_INDEX", "BRAIN_EXCLUSION_FINGERPRINT",
							"OWNER_ID", "OWNER_TYPE", "THREAD_KEY", "PERSON_ID"),

					// work
					OwlIndex.of("WORK_ITEM_OWNER_INDEX", "WORK_ITEM", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("WORK_ITEM_ITEM_ID_INDEX", "WORK_ITEM", "OWNER_ID", "OWNER_TYPE", "ITEM_ID"),
					OwlIndex.of("WORK_ITEM_THREAD_ID_INDEX", "WORK_ITEM", "OWNER_ID", "OWNER_TYPE", "THREAD_ID"),
					OwlIndex.of("WORK_ITEM_DEDUPE_KEY_INDEX", "WORK_ITEM", "OWNER_ID", "OWNER_TYPE", "DEDUPE_KEY"),
					OwlIndex.of("WORK_ITEM_HISTORY_OWNER_INDEX", "WORK_ITEM_HISTORY", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("WORK_ITEM_HISTORY_ITEM_ID_INDEX", "WORK_ITEM_HISTORY", "OWNER_ID", "OWNER_TYPE",
							"ITEM_ID"),
					OwlIndex.of("WORK_OPEN_ROOM_OWNER_INDEX", "WORK_OPEN_ROOM", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("WORK_OPEN_ROOM_THREAD_ID_INDEX", "WORK_OPEN_ROOM", "OWNER_ID", "OWNER_TYPE",
							"THREAD_ID")));

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			if (conn != null && collaborationDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}
}
