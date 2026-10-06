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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.javatuples.Pair;
import prerna.auth.User;

/** Searches saved metadata only, with the same owner boundary as Brain reads. */
public final class CollaborationSearchUtils {
	public static final int DEFAULT_LIMIT = 30;
	public static final int MAX_LIMIT = 100;

	private CollaborationSearchUtils() { }

	public static Map<String, Object> search(User user, String query, int limit, int offset) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		if (limit < 1 || limit > MAX_LIMIT || offset < 0) {
			throw new IllegalArgumentException("limit must be 1-100 and offset must be non-negative");
		}
		String term = query == null ? "" : query.strip().toLowerCase(Locale.ROOT);
		Map<String, Object> page = new LinkedHashMap<>();
		if (term.isEmpty()) {
			page.put("items", List.of());
			page.put("total", 0);
			return page;
		}
		// Escape SQL LIKE metacharacters: searches are literal substrings.
		String like = "%" + term.replace("!", "!!").replace("%", "!%").replace("_", "!_") + "%";
		List<Object> params = new ArrayList<>();
		String owned = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ";
		String union = "SELECT 'thread' AS KIND, THREAD_ID AS ID, COALESCE(SUBJECT, '(no subject)') AS NAME, SOURCE AS DETAIL "
				+ "FROM BRAIN_THREAD" + owned + "LOWER(SUBJECT) LIKE ? ESCAPE '!' "
				+ "UNION ALL SELECT 'person' AS KIND, PERSON_ID AS ID, COALESCE(DISPLAY_NAME, EMAIL_NORM, 'Unknown') AS NAME, EMAIL_NORM AS DETAIL "
				+ "FROM BRAIN_PERSON" + owned + "(LOWER(DISPLAY_NAME) LIKE ? ESCAPE '!' OR LOWER(EMAIL_NORM) LIKE ? ESCAPE '!') "
				+ "UNION ALL SELECT 'topic' AS KIND, TOPIC_ID AS ID, NAME, STATUS AS DETAIL "
				+ "FROM BRAIN_TOPIC" + owned + "LOWER(NAME) LIKE ? ESCAPE '!'";
		params.addAll(List.of(owner.getValue0(), owner.getValue1(), like));
		params.addAll(List.of(owner.getValue0(), owner.getValue1(), like, like));
		params.addAll(List.of(owner.getValue0(), owner.getValue1(), like));
		String from = " FROM (" + union + ") matches";
		page.put("items", CollaborationDbUtils.query(CollaborationDbUtils.page(
				"SELECT KIND, ID, NAME, DETAIL" + from + " ORDER BY LOWER(NAME), KIND, ID", limit, offset), rs -> {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("kind", CollaborationDbUtils.getString(rs, "KIND"));
			row.put("id", CollaborationDbUtils.getString(rs, "ID"));
			row.put("name", CollaborationDbUtils.getString(rs, "NAME"));
			row.put("detail", CollaborationDbUtils.getString(rs, "DETAIL"));
			return row;
		}, params.toArray()));
		page.put("total", CollaborationDbUtils.count("SELECT COUNT(*)" + from, params.toArray()));
		return page;
	}

	/** Detail loading for a result outside the initial session batch, including thread context. */
	public static Map<String, Object> getRecord(User user, String kind, String id) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		if (id == null || id.isBlank()) throw new IllegalArgumentException("Record id is required");
		Map<String, Object> result = new LinkedHashMap<>();
		List<Map<String, Object>> topics = new ArrayList<>();
		List<Map<String, Object>> people = new ArrayList<>();
		List<Map<String, Object>> threads = new ArrayList<>();
		result.put("topics", topics);
		result.put("people", people);
		result.put("threads", threads);
		result.put("items", List.of());
		result.put("workspaces", Map.of("items", List.of(), "total", 0));
		if ("topic".equals(kind)) {
			topics.add(BrainTopicUtils.getTopic(user, id));
		} else if ("person".equals(kind)) {
			people.add(BrainPeopleUtils.getPerson(owner.getValue0(), owner.getValue1(), id));
		} else if ("thread".equals(kind)) {
			Map<String, Object> thread = BrainThreadUtils.getThread(user, id);
			threads.add(thread);
			for (String topicId : ids(thread.get("topicLinks"), "topicId")) {
				topics.add(BrainTopicUtils.getTopic(user, topicId));
			}
			for (String personId : ids(thread.get("participants"), "personId")) {
				people.add(BrainPeopleUtils.getPerson(owner.getValue0(), owner.getValue1(), personId));
			}
			result.put("workspaces", WorkWorkspaceUtils.listWorkspaces(user, id));
			Map<String, Object> filter = Map.of("threadId", id);
			int count = WorkItemUtils.countItems(user, filter);
			if (count > 0) result.put("items", WorkItemUtils.listItems(user, filter, count, 0).get("items"));
		} else {
			throw new IllegalArgumentException("kind must be thread, person, or topic");
		}
		return result;
	}

	private static List<String> ids(Object value, String key) {
		if (!(value instanceof List<?> rows)) return List.of();
		return rows.stream().filter(Map.class::isInstance).map(Map.class::cast)
				.map(row -> row.get(key)).filter(String.class::isInstance).map(String.class::cast).distinct().toList();
	}
}
