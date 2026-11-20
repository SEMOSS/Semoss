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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

// Groups named onboarding topics into broader areas of work with one small model call; each topic is its own
// area when the call fails.
final class BrainTopicAreas {

	static final String INSTRUCTIONS = """
			You help one person organize the topics found in their email and chats into broader areas of work. Each topic
			has an id, a name, one sentence about it and how many conversations it has.

			Put topics that are part of the same ongoing work into one area: the same client, product, project, team or
			responsibility. A one-time event (an outage, a launch, a review, a visit) belongs in the area of the ongoing
			work it is part of. Keep topics in separate areas when they are different work. An area can hold one topic.

			Name each area the way the person would name a folder, in 2 to 5 words, after the ongoing work; never after one
			event, a month, a date or a person. Write "about": one plain sentence, under 25 words, saying what mail belongs
			in the area.

			Use every topic id exactly once.
			""";

	record Topic(String name, String about, int conversations) {
	}

	// members index the input topics
	record Area(String name, String about, List<Integer> members) {
	}

	private BrainTopicAreas() {
	}

	static List<Area> group(BrainTopicVotes.Caller caller, List<Topic> topics) {
		if (topics.size() < 2) {
			return single(topics);
		}
		List<String> ids = new ArrayList<>();
		List<Map<String, Object>> cards = new ArrayList<>();
		for (int i = 0; i < topics.size(); i++) {
			ids.add("t" + (i + 1));
			Map<String, Object> card = new LinkedHashMap<>();
			card.put("id", ids.get(i));
			card.put("name", topics.get(i).name());
			if (topics.get(i).about() != null) {
				card.put("about", topics.get(i).about());
			}
			card.put("conversations", topics.get(i).conversations());
			cards.add(card);
		}
		Map<String, Object> params = Map.of("temperature", 0, "schema", schema(ids), "max_tokens", 3000);
		try {
			List<Area> areas = validate(caller.ask(new Gson().toJson(Map.of("topics", cards)), INSTRUCTIONS, params),
					ids, topics);
			return areas == null ? single(topics) : areas;
		} catch (RuntimeException e) {
			// private prompts and replies are never logged here
			return single(topics);
		}
	}

	static List<Area> single(List<Topic> topics) {
		List<Area> out = new ArrayList<>();
		for (int i = 0; i < topics.size(); i++) {
			out.add(new Area(topics.get(i).name(), topics.get(i).about(), List.of(i)));
		}
		return out;
	}

	static Map<String, Object> schema(List<String> ids) {
		Map<String, Object> area = Map.of("type", "object", "additionalProperties", false, "required",
				List.of("name", "about", "topics"), "properties",
				Map.of("name", Map.of("type", "string"), "about", Map.of("type", "string"), "topics",
						Map.of("type", "array", "minItems", 1, "items", Map.of("type", "string", "enum", ids))));
		return Map.of("type", "object", "additionalProperties", false, "required", List.of("areas"), "properties",
				Map.of("areas", Map.of("type", "array", "minItems", 1, "items", area)));
	}

	// a topic the reply leaves out becomes its own area; one listed twice stays in its first
	static List<Area> validate(String reply, List<String> ids, List<Topic> topics) {
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
			if (!root.isJsonObject() || !(root.getAsJsonObject().get("areas") instanceof JsonElement list)
					|| !list.isJsonArray()) {
				return null;
			}
			Set<Integer> used = new HashSet<>();
			List<Area> out = new ArrayList<>();
			for (JsonElement element : list.getAsJsonArray()) {
				if (!element.isJsonObject()) {
					return null;
				}
				JsonObject row = element.getAsJsonObject();
				String name = string(row, "name");
				String about = string(row, "about");
				if (name == null || name.isBlank() || name.length() > 60 || name.trim().split("\\s+").length > 6
						|| !(row.get("topics") instanceof JsonElement members) || !members.isJsonArray()) {
					return null;
				}
				List<Integer> kept = new ArrayList<>();
				for (JsonElement member : members.getAsJsonArray()) {
					int index = member.isJsonPrimitive() ? ids.indexOf(member.getAsString()) : -1;
					if (index >= 0 && used.add(index)) {
						kept.add(index);
					}
				}
				if (!kept.isEmpty()) {
					// one topic alone keeps its own name and description
					out.add(kept.size() == 1 ? new Area(topics.get(kept.get(0)).name(), topics.get(kept.get(0)).about(), kept)
							: new Area(name.trim(), about == null || about.isBlank() || about.length() > 300 ? null : about.trim(), kept));
				}
			}
			for (int i = 0; i < topics.size(); i++) {
				if (!used.contains(i)) {
					out.add(new Area(topics.get(i).name(), topics.get(i).about(), List.of(i)));
				}
			}
			return out;
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static String string(JsonObject row, String key) {
		JsonElement value = row.get(key);
		return value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString() ? value.getAsString()
				: null;
	}
}
