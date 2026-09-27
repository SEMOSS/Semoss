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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.ITypeSafeEngine;
import prerna.om.Insight;

// Jev (TypeSafe) models: typed choice / noul / score questions. Tuned on brain-mail-v1: no "none of these"
// choice (it drew most answers), and "is this only informing?" beats "does it ask me?".
// With one or two topics the policy adds a "Something else" choice itself; one topic alone is never scored.
final class JevBrainClassifier implements BrainClassifier {

	private static final String[] URGENCY = { "Whenever", "This week", "Today", "Right now" };

	private final String engineId;
	private final ITypeSafeEngine engine;

	JevBrainClassifier(String engineId, ITypeSafeEngine engine) {
		this.engineId = engineId;
		this.engine = engine;
	}

	@Override
	public String version() {
		return "jev-v1:" + engineId;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Scores score(ThreadInput thread, List<TopicOption> topics, Insight insight) {
		Map<String, Object> questions = new LinkedHashMap<>();
		// choice labels are topic names; a repeated name gets its id so labels stay unique
		Map<String, String> labelToId = new LinkedHashMap<>();
		if (topics.size() > 1) {
			Map<String, Object> criteria = new LinkedHashMap<>();
			for (TopicOption topic : topics) {
				String label = labelToId.containsKey(topic.name()) ? topic.name() + " (" + topic.id() + ")" : topic.name();
				labelToId.put(label, topic.id());
				criteria.put(label, topic.description());
			}
			questions.put("topic", question("choice", "Which of these work topics is this email thread about?", criteria));
		}
		questions.put("fyi", question("noul", "Is the newest message only informing (an update, heads-up, approval, "
				+ "or sign-off) with nothing for anyone to do?", null));
		questions.put("automated", question("noul", "Is this an automated or bulk message (newsletter, notification, "
				+ "no-reply, alert) rather than a person writing?", null));
		questions.put("urgency", question("score", "How urgently does the newest message need a response?",
				List.of(URGENCY)));

		Map<String, Object> answers = (Map<String, Object>) engine.evaluate(state(thread), questions, insight, null)
				.getResponse().get("answers");
		Map<String, Double> topicScores = new LinkedHashMap<>();
		Map<String, Object> topic = map(answers.get("topic"));
		if (topic != null && topic.get("probabilities") instanceof Map<?, ?> probs) {
			for (Map.Entry<?, ?> e : probs.entrySet()) {
				String id = labelToId.get(String.valueOf(e.getKey()));
				if (id != null && e.getValue() instanceof Number n) {
					topicScores.put(id, n.doubleValue());
				}
			}
		}
		Map<String, Object> urgency = map(answers.get("urgency"));
		return new Scores(topicScores, noul(answers, "fyi"), noul(answers, "automated"),
				urgency != null && urgency.get("score") instanceof Number n ? n.doubleValue() : 0, answers);
	}

	// the thread as Jev state; Jev did best on the newest message alone (two messages and the owner's name
	// dropped topic accuracy in the fixture eval), so earlier messages stay out
	static Map<String, Object> state(ThreadInput thread) {
		Map<String, Object> state = new LinkedHashMap<>();
		state.put("subject", thread.subject());
		state.put("participants", thread.participants());
		state.put("messageCount", thread.messages().size());
		if (!thread.messages().isEmpty()) {
			Message m = thread.messages().get(thread.messages().size() - 1);
			Map<String, Object> message = new LinkedHashMap<>();
			message.put("from", m.from());
			message.put("to", m.to());
			message.put("cc", m.cc());
			message.put("text", m.text());
			state.put("newestMessage", message);
		}
		return state;
	}

	private static Map<String, Object> question(String type, String instructions, Object criteria) {
		Map<String, Object> q = new LinkedHashMap<>();
		q.put("type", type);
		q.put("instructions", instructions);
		if (criteria != null) {
			q.put("criteria", criteria);
		}
		return q;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return value instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
	}

	private static double noul(Map<String, Object> answers, String key) {
		Map<String, Object> a = map(answers.get(key));
		return a != null && a.get("noul") instanceof Number n ? n.doubleValue() : 0;
	}
}
