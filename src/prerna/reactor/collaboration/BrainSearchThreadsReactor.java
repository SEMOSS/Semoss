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
package prerna.reactor.collaboration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.collaboration.BrainThreadFinder;
import prerna.collaboration.BrainTopicBrief;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSearchThreads(topic=["Air Force AIMES"], query=["renewal pricing"], person=["priya"], from=["2026-09-01"], to=["2026-10-05"], limit=[10]);
public class BrainSearchThreadsReactor extends AbstractCollaborationReactor {

	private static final String TOPIC_ID = "topicId";
	private static final String TOPIC = "topic";
	private static final String QUERY = "query";
	private static final String PERSON = "person";
	private static final String FROM = "from";
	private static final String TO = "to";
	private static final String LIMIT = "limit";
	private static final String ALL_TOPICS = "allTopics";

	public BrainSearchThreadsReactor() {
		this.keysToGet = new String[] { TOPIC_ID, TOPIC, QUERY, PERSON, FROM, TO, LIMIT, ALL_TOPICS };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (ALL_TOPICS.equals(key)) {
			return MCP_KEY_TYPE.BOOLEAN;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String topicId = getString(TOPIC_ID);
		String topic = getString(TOPIC);
		// a topic limits the search to the threads in it
		if ((topicId != null && !topicId.isBlank()) || (topic != null && !topic.isBlank())) {
			return mapResult(BrainThreadFinder.searchTopic(user, topicId, topic, getString(QUERY), getString(PERSON),
					getString(FROM), getString(TO), getIntFromKeyOrCurRow(LIMIT)));
		}
		// in a chat about topics the search stays in them, unless the assistant asks for everything
		if (!Boolean.TRUE.equals(getBoolean(ALL_TOPICS))) {
			List<String> chatTopics = chatTopics(user);
			if (!chatTopics.isEmpty()) {
				Map<String, Object> result = BrainThreadFinder.searchTopics(user, chatTopics, getString(QUERY),
						getString(PERSON), getString(FROM), getString(TO), getIntFromKeyOrCurRow(LIMIT));
				result = new LinkedHashMap<>(result);
				result.put("scope", "this chat's topics; pass allTopics to search all of the owner's mail");
				return mapResult(result);
			}
		}
		return mapResult(BrainThreadFinder.searchAll(user, getString(QUERY), getString(PERSON), getString(FROM),
				getString(TO), getIntFromKeyOrCurRow(LIMIT)));
	}

	// the topics of the chat this runs in; none outside an owner's chat, or when the chat has no topic yet
	private List<String> chatTopics(User user) {
		String roomId = this.insight.getRoomId();
		if (roomId == null) {
			return List.of();
		}
		try {
			return BrainTopicBrief.chatTopics(user, roomId);
		} catch (RuntimeException e) {
			return List.of();
		}
	}

	@Override
	public String getReactorDescription() {
		return "Finds threads in the owner's Brain by words in the subject, summary, or people, by person, or by date, "
				+ "newest first; each result names its topics. Pass a topic to look only inside it, and with nothing "
				+ "else it lists that topic's newest threads. Inside a topic it also reads the messages and a model "
				+ "picks the passages that answer the question, so ask in a full sentence with the question's own "
				+ "words; a match shows the quoted passage, who wrote it, and how sure the model is. In a chat that "
				+ "has topics it looks inside them by default; set allTopics only when the owner asks for mail "
				+ "outside the topic, because message text is then not searched. Read a match with "
				+ "BrainGetThreadMessages. Never-ingest and muted content is left out";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (TOPIC_ID.equals(key)) {
			return "Only threads in this topic, by id from ListTopics";
		} else if (TOPIC.equals(key)) {
			return "Only threads in this topic, by name, when the id is not known; must match one topic";
		} else if (QUERY.equals(key)) {
			return "Optional. Words to look for in the thread subject, summary, and people";
		} else if (PERSON.equals(key)) {
			return "Only threads with a person whose name or email contains this";
		} else if (FROM.equals(key)) {
			return "Only threads with a message on or after this date (YYYY-MM-DD)";
		} else if (TO.equals(key)) {
			return "Only threads with a message on or before this date (YYYY-MM-DD)";
		} else if (LIMIT.equals(key)) {
			return "Best N threads, default 10, at most 25";
		} else if (ALL_TOPICS.equals(key)) {
			return "true only when the owner asks for mail outside this chat's topics; message text is not searched then";
		}
		return super.getDescriptionForKey(key);
	}
}
