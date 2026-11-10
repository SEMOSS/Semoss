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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

// Finds conversations that only say someone is away (time off, out of office) so topic discovery leaves them out.
// The model judges each conversation; no subject matching.
final class BrainTopicNotices {

	static final String INSTRUCTIONS = """
			Below are conversations from one person's mail and chats. Each has an id, its subject, how many messages it
			has, and "calendar": true when it is a calendar item (an invite, a hold, or an accept, decline or cancel reply).

			List the ids of the conversations that only say when someone is away or unavailable: time off, out of office,
			holidays, sick days, appointments, personal holds on a calendar, and replies to those. Do not list anything
			about a project, client, deal or piece of work, even when it mentions someone being away, and do not list a
			meeting where work is discussed. When unsure, do not list it. Return an empty list when none qualify.
			""";

	// conversations per model call
	static final int BATCH = 100;
	private static final int PARALLEL = 4;

	record Card(String threadId, String subject, int messages, boolean calendar) {
	}

	private BrainTopicNotices() {
	}

	static Map<String, Object> schema(List<String> ids) {
		return Map.of("type", "object", "additionalProperties", false, "required", List.of("notices"), "properties",
				Map.of("notices", Map.of("type", "array", "items", Map.of("type", "string", "enum", ids))));
	}

	// Thread ids most votes call an away notice. A batch with too few good answers flags nothing, so a failed
	// call keeps mail in discovery rather than dropping it.
	static Set<String> judge(BrainTopicVotes.Caller caller, List<Card> cards, BrainTopicStructure.Settings settings) {
		List<List<Card>> batches = new ArrayList<>();
		for (int start = 0; start < cards.size(); start += BATCH) {
			batches.add(cards.subList(start, Math.min(cards.size(), start + BATCH)));
		}
		if (batches.isEmpty()) {
			return Set.of();
		}
		ExecutorService pool = Executors.newFixedThreadPool(PARALLEL);
		try {
			List<List<Future<Set<String>>>> running = new ArrayList<>();
			for (List<Card> batch : batches) {
				List<Future<Set<String>>> votes = new ArrayList<>();
				for (int v = 0; v < settings.votes(); v++) {
					votes.add(pool.submit(() -> vote(caller, batch, settings)));
				}
				running.add(votes);
			}
			Set<String> out = new HashSet<>();
			for (int b = 0; b < batches.size(); b++) {
				Map<String, Integer> counts = new HashMap<>();
				for (Future<Set<String>> future : running.get(b)) {
					Set<String> flagged = future.get();
					if (flagged != null) {
						flagged.forEach(id -> counts.merge(id, 1, Integer::sum));
					}
				}
				counts.forEach((id, n) -> {
					if (n >= settings.dropAt()) {
						out.add(id);
					}
				});
			}
			return out;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return Set.of();
		} catch (Exception e) {
			return Set.of();
		} finally {
			pool.shutdownNow();
		}
	}

	// one vote on one batch: the flagged thread ids, or null when no valid answer came back
	private static Set<String> vote(BrainTopicVotes.Caller caller, List<Card> batch,
			BrainTopicStructure.Settings settings) {
		Map<String, String> byId = new LinkedHashMap<>();
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Card card : batch) {
			String id = "c" + (rows.size() + 1);
			byId.put(id, card.threadId());
			rows.add(Map.of("id", id, "subject", card.subject(), "messages", card.messages(), "calendar",
					card.calendar()));
		}
		List<String> ids = List.copyOf(byId.keySet());
		String prompt = new Gson().toJson(Map.of("conversations", rows));
		Map<String, Object> parameters = Map.of("temperature", 0, "schema", schema(ids), "max_tokens", 2000);
		for (int attempt = 0; attempt < settings.attempts(); attempt++) {
			try {
				Set<String> flagged = validate(caller.ask(prompt, INSTRUCTIONS, parameters), byId.keySet());
				if (flagged != null) {
					Set<String> threads = new HashSet<>();
					flagged.forEach(id -> threads.add(byId.get(id)));
					return threads;
				}
			} catch (RuntimeException e) {
				// a failed call uses up its attempt; private prompts and replies are never logged here
			}
		}
		return null;
	}

	static Set<String> validate(String reply, Set<String> ids) {
		if (reply == null) {
			return null;
		}
		String text = reply.replaceAll("(?s)<think>.*?</think>", "").trim();
		if (text.startsWith("```")) {
			int newline = text.indexOf('\n');
			int end = text.lastIndexOf("```");
			text = newline >= 0 && end > newline ? text.substring(newline + 1, end).trim() : text;
		}
		try {
			JsonElement root = JsonParser.parseString(text);
			if (!root.isJsonObject() || !(root.getAsJsonObject().get("notices") instanceof JsonElement notices)
					|| !notices.isJsonArray()) {
				return null;
			}
			Set<String> out = new HashSet<>();
			for (JsonElement element : notices.getAsJsonArray()) {
				if (!element.isJsonPrimitive() || !ids.contains(element.getAsString())) {
					return null;
				}
				out.add(element.getAsString());
			}
			return out;
		} catch (RuntimeException e) {
			return null;
		}
	}
}
