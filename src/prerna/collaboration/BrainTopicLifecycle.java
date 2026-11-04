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

/** Topic lifecycle policy shared by filing, attention queries, and chat context. */
final class BrainTopicLifecycle {

	private BrainTopicLifecycle() { }

	static void upgrade(Connection connection) throws SQLException {
		// Old dormant rows were automatic idle labels, not explicit requests to hide work.
		CollaborationDbUtils.update(connection, "UPDATE BRAIN_TOPIC SET STATUS = ? WHERE STATUS = ?", "active", "dormant");
		CollaborationDbUtils.update(connection, "UPDATE BRAIN_CHANGE SET SNAPSHOT_JSON = NULL WHERE ENTITY_TYPE = ? "
				+ "AND FIELD = ? AND SNAPSHOT_JSON IS NOT NULL", "topic", "delete");
	}

	static boolean canFile(String status) {
		return BrainTopicUtils.ACTIVE.equals(status) || BrainTopicUtils.HIBERNATED.equals(status)
				|| BrainTopicUtils.DORMANT.equals(status);
	}

	static void requireFiling(String ownerId, String ownerType, String topicId) {
		if (!canFile(lockStatus(ownerId, ownerType, topicId))) {
			throw new IllegalArgumentException("That topic is inactive; restore it before filing new threads");
		}
	}

	static String lockStatus(String ownerId, String ownerType, String topicId) {
		String status = CollaborationDbUtils.queryOne("SELECT STATUS FROM BRAIN_TOPIC WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND TOPIC_ID = ? FOR UPDATE", rs -> rs.getString(1), ownerId, ownerType, topicId);
		if (status == null) throw new IllegalArgumentException("Topic not found");
		return status;
	}

	// The relation is internal SQL, never caller input. Untagged work stays visible; any active topic wins.
	private static String attention(String alias, String relation) {
		String topics = " FROM BRAIN_TOPIC lifecycle_topic WHERE lifecycle_topic.OWNER_ID = " + alias
				+ ".OWNER_ID AND lifecycle_topic.OWNER_TYPE = " + alias + ".OWNER_TYPE AND (" + relation + ")";
		return "(NOT EXISTS (SELECT 1" + topics + ") OR EXISTS (SELECT 1" + topics
				+ " AND lifecycle_topic.STATUS IN ('active', 'dormant')))";
	}

	static String itemAttention(String alias) {
		return attention(alias, threadRelation(alias));
	}

	static String taskAttention(String alias) {
		return attention(alias, "EXISTS (SELECT 1 FROM WORK_TASK_TOPIC lifecycle_link WHERE lifecycle_link.OWNER_ID = "
				+ alias + ".OWNER_ID AND lifecycle_link.OWNER_TYPE = " + alias
				+ ".OWNER_TYPE AND lifecycle_link.TASK_ID = " + alias
				+ ".TASK_ID AND lifecycle_link.TOPIC_ID = lifecycle_topic.TOPIC_ID)");
	}

	static String stepAttention(String alias) {
		return attention(alias, threadRelation(alias));
	}

	private static String threadRelation(String alias) {
		return "lifecycle_topic.TOPIC_ID = " + alias + ".LINK_TOPIC_ID OR EXISTS (SELECT 1 FROM BRAIN_THREAD_TOPIC "
				+ "lifecycle_link WHERE lifecycle_link.OWNER_ID = " + alias
				+ ".OWNER_ID AND lifecycle_link.OWNER_TYPE = " + alias
				+ ".OWNER_TYPE AND lifecycle_link.THREAD_ID = " + alias
				+ ".THREAD_ID AND (lifecycle_link.SOURCE IS NULL OR lifecycle_link.SOURCE <> 'suggested') "
				+ "AND lifecycle_link.TOPIC_ID = lifecycle_topic.TOPIC_ID)";
	}
}
