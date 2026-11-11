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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.collaboration.BrainThreadFinder;
import prerna.collaboration.BrainTopicBrief;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSearchThreads(topic=["Air Force AIMES"], query=["renewal pricing"], person=["priya"], from=["2026-09-01"], to=["2026-10-05"], limit=[10]);
public class BrainSearchThreadsReactor extends AbstractCollaborationReactor {

	private static final Logger classLogger = LogManager.getLogger(BrainSearchThreadsReactor.class);

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
		} catch (SemossPixelException e) {
			throw e;
		} catch (RuntimeException e) {
			// without the chat's topics this becomes a search of all mail, so the failure must not go unseen
			classLogger.warn("The topics of chat {} could not be read, so the search covers all mail", roomId, e);
			return List.of();
		}
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Finds threads in the owner's Brain by words in the subject, summary, or people, by person, or by date, "
						+ "newest first; each result names its topics. Pass a topic to look only inside it, and with nothing "
						+ "else it lists that topic's newest threads. Inside a topic it also reads the messages and a model "
						+ "picks the passages that answer the question, so ask in a full sentence with the question's own "
						+ "words; a match shows the quoted passage, who wrote it, and how sure the model is. In a chat that has topics it looks "
						+ "inside them unless allTopics is set. Read a match with BrainGetThreadMessages. Never-ingest and "
						+ "muted content is left out")
				.key(TOPIC_ID, "Only threads in this topic, by id from ListTopics")
				.key(TOPIC, "Only threads in this topic, by name, when the id is not known; must match one topic")
				.key(QUERY, "Optional. The owner's question exactly as asked, as a whole sentence and not shortened to "
						+ "keywords; it is matched against the thread subject, summary, and people, and inside a topic "
						+ "against the message text")
				.key(PERSON, "Only threads with a person whose name or email contains this")
				.key(FROM, "Only threads with a message on or after this date (YYYY-MM-DD)")
				.key(TO, "Only threads with a message on or before this date (YYYY-MM-DD)")
				.key(LIMIT, "Best N threads, default 10, at most 25")
				.key(ALL_TOPICS, key -> key.type(Boolean.class).description(
						"true to search all of the owner's threads even though this chat has topics"));
	}
}
