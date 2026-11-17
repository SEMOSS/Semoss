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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Migrates work item room links from the single WORK_TASK.ROOM_ID column to the
 * WORK_TASK_ROOM link table, enabling multiple chats per work item. Each
 * non-null ROOM_ID is moved to a new row in WORK_TASK_ROOM, preserving
 * timestamps, and cleared in the same transaction, so running it again moves
 * nothing. The old ROOM_ID column can then be dropped.
 */
final class WorkTaskRoomMigration {

	private static final Logger classLogger = LogManager.getLogger(WorkTaskRoomMigration.class);

	private WorkTaskRoomMigration() {
	}

	static void run() {
		String left = " WHERE ROOM_ID IS NOT NULL";
		CollaborationDbUtils.inTransaction(conn -> {
			// locks the rows first, so two servers starting together cannot both copy them
			CollaborationDbUtils.update(conn, "UPDATE WORK_TASK SET UPDATED_AT = UPDATED_AT" + left);
			int moved = CollaborationDbUtils.update(conn,
					"INSERT INTO WORK_TASK_ROOM (OWNER_ID, OWNER_TYPE, TASK_ID, ROOM_ID, CREATED_AT) "
							+ "SELECT OWNER_ID, OWNER_TYPE, TASK_ID, ROOM_ID, UPDATED_AT FROM WORK_TASK" + left);
			// cleared with the copy, so a later boot has nothing left to move and a chat
			// taken off an item stays off
			CollaborationDbUtils.update(conn, "UPDATE WORK_TASK SET ROOM_ID = NULL, UPDATED_AT = UPDATED_AT" + left);
			if (moved > 0) {
				classLogger.info("Migrated {} work item room links to WORK_TASK_ROOM table", moved);
			}
		});
	}

	/**
	 * Drops the WORK_TASK.ROOM_ID column after migration is complete. Call only
	 * after confirming migration has completed successfully.
	 */
	static void dropOldColumn() {
		try {
			// This is database-specific; handle multiple DB types as needed
			CollaborationDbUtils.update("ALTER TABLE WORK_TASK DROP COLUMN ROOM_ID");
			classLogger.info("Dropped WORK_TASK.ROOM_ID column");
		} catch (Exception e) {
			classLogger.warn("Could not drop WORK_TASK.ROOM_ID column; may not be supported by this database", e);
		}
	}
}
