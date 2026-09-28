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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

// topic proposals from the platform text model (COLLAB_LLM_ENGINE_ID): it groups working threads and names
// them from headers only (subject, organisations, counts), after keep-out. Code checks every answer.
final class BrainTopicModel {

	static final int MAX_THREADS = 300;
	private static final int MAX_TOPICS = 15;
	private static final int MAX_WORDS = 5;

	private static final String INSTRUCTIONS = """
			You organise one person's work email into topics. You get their recent email threads (subject only,
			no bodies), the organisations on each thread, how many messages it has, whether the person wrote on it
			(you) and whether one of their VIPs is on it (vip).

			Group the threads into the topics this person actually works on: projects, client engagements, deals,
			proposals, recurring workstreams, hiring, and similar. Name each topic the way a colleague would,
			in 2 to 5 words, after the project, client, or piece of work it is about. Threads the person wrote
			on, and VIP threads, matter most.

			Leave out: newsletters and digests, announcements to large lists, calendar notices, system and
			approval notices, timesheets and expenses, training reminders, and personal mail. Do not make topics
			named after a person, a date, or a generic word (Update, Reminder, Meeting, Request, Action Required).

			Rules: every topic has at least 2 threads; a thread is in at most one topic; at most 15 topics;
			organisation is one of the given organisations, or "internal".

			Reply with JSON only:
			{"topics": [{"name": "...", "organisation": "...", "threads": ["t1", "t7"], "why": "one short sentence"}]}
			""";

	private BrainTopicModel() {
	}

	record Proposal(String name, String organisation, List<String> threadIds, String why) {
	}

	/** The platform text model, or null when none is set; the caller needs access to it. */
	static String engine(User user) {
		String id = Utility.getDIHelperProperty(Constants.COLLAB_LLM_ENGINE_ID);
		if (id == null || id.isBlank()) {
			return null;
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, id.trim())) {
			throw new IllegalArgumentException("You do not have access to the topic model (" + id.trim()
					+ "); ask an admin to share it with you");
		}
		return id.trim();
	}

	/**
	 * threads: id, subject, orgs (labels), messages, you, vip; most important first, at most MAX_THREADS.
	 * Returns checked proposals with real thread ids.
	 */
	static List<Proposal> propose(User user, String engineId, List<Map<String, Object>> threads,
			List<String> organisations, Set<String> takenNames) {
		IModelEngine model = Utility.getModel(engineId);
		if (model == null) {
			throw new IllegalArgumentException("Topic model " + engineId + " could not be loaded");
		}
		// short ids keep the prompt small and make made-up ids easy to catch
		Map<String, String> byShort = new LinkedHashMap<>();
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Map<String, Object> t : threads.subList(0, Math.min(MAX_THREADS, threads.size()))) {
			String shortId = "t" + (byShort.size() + 1);
			byShort.put(shortId, (String) t.get("id"));
			Map<String, Object> row = new LinkedHashMap<>(t);
			row.put("id", shortId);
			rows.add(row);
		}
		Map<String, Object> input = new LinkedHashMap<>();
		input.put("organisations", organisations);
		input.put("threads", rows);
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("temperature", 0);
		Insight insight = new Insight();
		insight.setUser(user);
		String reply = model.ask(CollaborationDbUtils.toJson(input), INSTRUCTIONS, insight, params).getStringResponse();

		List<Proposal> out = new ArrayList<>();
		Set<String> used = new HashSet<>();
		Set<String> names = new HashSet<>(takenNames);
		Object topics = parse(reply).get("topics");
		if (!(topics instanceof List<?> list)) {
			throw new IllegalStateException("The topic model did not return a topic list");
		}
		for (Object item : list) {
			if (!(item instanceof Map<?, ?> m) || out.size() >= MAX_TOPICS) {
				continue;
			}
			String name = m.get("name") == null ? "" : String.valueOf(m.get("name")).trim();
			if (name.isEmpty() || name.length() > 60 || name.split("\\s+").length > MAX_WORDS
					|| !names.add(name.toLowerCase())) {
				continue;
			}
			List<String> ids = new ArrayList<>();
			if (m.get("threads") instanceof List<?> given) {
				for (Object g : given) {
					String real = byShort.get(String.valueOf(g).trim());
					// unknown or already used ids are dropped, not trusted
					if (real != null && used.add(real)) {
						ids.add(real);
					}
				}
			}
			if (ids.size() < 2) {
				used.removeAll(ids);
				continue;
			}
			String org = m.get("organisation") == null ? null : String.valueOf(m.get("organisation")).trim();
			String why = m.get("why") == null ? "" : String.valueOf(m.get("why")).trim();
			out.add(new Proposal(name, org, ids, why.length() > 200 ? why.substring(0, 200) : why));
		}
		return out;
	}

	// the JSON object in the reply, ignoring code fences or stray text around it
	private static Map<String, Object> parse(String reply) {
		int start = reply == null ? -1 : reply.indexOf('{');
		int end = reply == null ? -1 : reply.lastIndexOf('}');
		if (start < 0 || end <= start) {
			throw new IllegalStateException("The topic model did not return JSON");
		}
		Map<String, Object> answer = CollaborationDbUtils.parseMap(reply.substring(start, end + 1));
		if (answer == null) {
			throw new IllegalStateException("The topic model did not return JSON");
		}
		return answer;
	}
}
