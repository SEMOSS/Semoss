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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// Broad areas in the onboarding draft. An area of several topics is one draft topic (its first part, which keeps
// its own profile under "own") until the owner keeps the parts separate.
final class BrainTopicReviewAreas {

	// the owner sees this many areas first; only these start kept
	static final int FIRST = 5;
	private static final List<String> OWN = List.of("name", "description", "about", "threadIds", "memberIds", "people",
			"domains", "sampleSubjects", "suggestedTerms", "youWrote", "vipThreads");

	private BrainTopicReviewAreas() {
	}

	/** Draft areas from the model's grouping, most involved first; topics of a shared area are combined. */
	static List<Map<String, Object>> build(List<Map<String, Object>> topics, List<BrainTopicAreas.Area> grouped) {
		List<Map<String, Object>> areas = new ArrayList<>();
		for (BrainTopicAreas.Area area : grouped) {
			List<Map<String, Object>> members = area.members().stream().map(topics::get).toList();
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("name", area.name());
			entry.put("about", Objects.toString(area.about(), ""));
			entry.put("topicKeys", members.stream().map(t -> (String) t.get("key")).toList());
			entry.put("suggested", members.stream().anyMatch(t -> Boolean.TRUE.equals(t.get("suggested"))));
			entry.put("split", false);
			entry.put("weight", members.stream().mapToInt(t -> number(t.get("youWrote")) + number(t.get("vipThreads"))).sum());
			entry.put("size", members.stream().mapToInt(t -> strings(t.get("threadIds")).size()).sum());
			areas.add(entry);
		}
		areas.sort(Comparator.<Map<String, Object>>comparingInt(a -> number(a.get("weight"))).reversed()
				.thenComparing(Comparator.<Map<String, Object>>comparingInt(a -> number(a.get("size"))).reversed()));
		for (int i = 0; i < areas.size(); i++) {
			Map<String, Object> area = areas.get(i);
			area.put("key", "area-" + (i + 1));
			boolean keep = i < FIRST && Boolean.TRUE.equals(area.get("suggested"));
			for (String key : strings(area.get("topicKeys"))) {
				Map<String, Object> topic = find(topics, key);
				topic.put("area", area.get("key"));
				topic.put("keep", keep);
			}
			if (strings(area.get("topicKeys")).size() > 1) {
				join(topics, List.of(), area);
			}
		}
		return areas;
	}

	/** Combine an area's parts into its first; people choices and conversation corrections follow. */
	static List<Map<String, Object>> join(List<Map<String, Object>> topics, List<Map<String, Object>> corrections,
			Map<String, Object> area) {
		List<Map<String, Object>> parts = strings(area.get("topicKeys")).stream().map(key -> find(topics, key)).toList();
		Map<String, Object> target = parts.get(0);
		if (!(target.get("own") instanceof Map<?, ?>)) {
			Map<String, Object> own = new LinkedHashMap<>();
			OWN.forEach(field -> own.put(field, target.get(field)));
			target.put("own", own);
		}
		Map<String, Object> own = map(target.get("own"));
		List<Map<String, Object>> sources = new ArrayList<>();
		sources.add(own);
		parts.subList(1, parts.size()).forEach(sources::add);
		Set<String> threads = new LinkedHashSet<>();
		Set<String> members = new LinkedHashSet<>();
		Map<String, Map<String, Object>> people = new LinkedHashMap<>();
		Set<String> domains = new LinkedHashSet<>();
		Set<String> terms = new LinkedHashSet<>();
		List<String> subjects = new ArrayList<>();
		int wrote = 0;
		int vip = 0;
		for (Map<String, Object> source : sources) {
			threads.addAll(strings(source.get("threadIds")));
			members.addAll(strings(source.get("memberIds")));
			maps(source.get("people")).forEach(person -> people.putIfAbsent((String) person.get("id"), person));
			domains.addAll(strings(source.get("domains")));
			terms.addAll(strings(source.get("suggestedTerms")));
			wrote += number(source.get("youWrote"));
			vip += number(source.get("vipThreads"));
		}
		// a couple of examples from each part, so the area reads as all of them
		for (int round = 0; subjects.size() < 6 && round < 3; round++) {
			for (Map<String, Object> source : sources) {
				List<String> examples = strings(source.get("sampleSubjects"));
				if (round < examples.size() && subjects.size() < 6 && !subjects.contains(examples.get(round))) {
					subjects.add(examples.get(round));
				}
			}
		}
		Set<String> removed = new LinkedHashSet<>();
		Map<String, Map<String, Object>> added = new LinkedHashMap<>();
		boolean keep = false;
		for (Map<String, Object> part : parts) {
			strings(part.get("removedPeople")).stream().filter(people::containsKey).forEach(removed::add);
			maps(part.get("addedPeopleInfo")).stream().filter(person -> !people.containsKey(person.get("id")))
					.forEach(person -> added.putIfAbsent((String) person.get("id"), person));
			keep |= Boolean.TRUE.equals(part.get("keep"));
		}
		target.put("name", area.get("name"));
		target.put("description", Objects.toString(area.get("about"), "").isEmpty() ? own.get("description") : area.get("about"));
		target.put("about", area.get("about"));
		target.put("threadIds", new ArrayList<>(threads));
		target.put("memberIds", new ArrayList<>(members));
		target.put("people", new ArrayList<>(people.values()));
		target.put("domains", domains.stream().limit(3).toList());
		target.put("suggestedTerms", terms.stream().limit(8).toList());
		target.put("sampleSubjects", subjects);
		target.put("youWrote", wrote);
		target.put("vipThreads", vip);
		target.put("removedPeople", new ArrayList<>(removed));
		target.put("addedPeople", new ArrayList<>(added.keySet()));
		target.put("addedPeopleInfo", new ArrayList<>(added.values()));
		target.put("keep", keep);
		for (Map<String, Object> part : parts.subList(1, parts.size())) {
			part.put("keep", false);
			part.put("mergedIntoKey", target.get("key"));
		}
		area.put("split", false);
		Set<String> keys = new LinkedHashSet<>(strings(area.get("topicKeys")));
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> correction : corrections) {
			Map<String, Object> row = new LinkedHashMap<>(correction);
			if (keys.contains(row.get("topicKey"))) {
				row.put("topicKey", target.get("key"));
			}
			if (out.stream().noneMatch(kept -> Objects.equals(kept.get("threadId"), row.get("threadId"))
					&& Objects.equals(kept.get("topicKey"), row.get("topicKey")))) {
				out.add(row);
			}
		}
		return out;
	}

	/** Keep an area's parts as their own topics again; the area's name stays for a later join. */
	static List<Map<String, Object>> split(List<Map<String, Object>> topics, List<Map<String, Object>> corrections,
			Map<String, Object> area) {
		List<Map<String, Object>> parts = strings(area.get("topicKeys")).stream().map(key -> find(topics, key)).toList();
		Map<String, Object> target = parts.get(0);
		Map<String, Object> own = map(target.get("own"));
		area.put("name", target.get("name"));
		area.put("about", Objects.toString(target.get("description"), ""));
		Set<String> removed = new LinkedHashSet<>(strings(target.get("removedPeople")));
		boolean keep = Boolean.TRUE.equals(target.get("keep"));
		own.forEach(target::put);
		target.remove("own");
		for (Map<String, Object> part : parts) {
			Set<String> here = new LinkedHashSet<>();
			maps(part.get("people")).forEach(person -> here.add((String) person.get("id")));
			part.put("removedPeople", removed.stream().filter(here::contains).toList());
			part.put("keep", keep);
			part.remove("mergedIntoKey");
		}
		Set<String> ownPeople = new LinkedHashSet<>();
		maps(target.get("people")).forEach(person -> ownPeople.add((String) person.get("id")));
		List<Map<String, Object>> addedInfo = maps(target.get("addedPeopleInfo")).stream()
				.filter(person -> !ownPeople.contains(person.get("id"))).toList();
		target.put("addedPeopleInfo", addedInfo);
		target.put("addedPeople", addedInfo.stream().map(person -> person.get("id")).toList());
		area.put("split", true);
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> correction : corrections) {
			Map<String, Object> row = new LinkedHashMap<>(correction);
			if (Objects.equals(row.get("topicKey"), target.get("key"))) {
				for (Map<String, Object> part : parts.subList(1, parts.size())) {
					if (strings(part.get("threadIds")).contains(row.get("threadId"))
							&& !strings(target.get("threadIds")).contains(row.get("threadId"))) {
						row.put("topicKey", part.get("key"));
						break;
					}
				}
			}
			out.add(row);
		}
		return out;
	}

	private static Map<String, Object> find(List<Map<String, Object>> topics, String key) {
		return topics.stream().filter(topic -> Objects.equals(key, topic.get("key"))).findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Draft topic not found"));
	}

	private static int number(Object value) {
		return value instanceof Number n ? n.intValue() : 0;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return value instanceof Map<?, ?> m ? (Map<String, Object>) m : new LinkedHashMap<>();
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> maps(Object value) {
		List<Map<String, Object>> out = new ArrayList<>();
		if (value instanceof List<?> list) {
			for (Object item : list) {
				if (item instanceof Map<?, ?> m) {
					out.add((Map<String, Object>) m);
				}
			}
		}
		return out;
	}

	private static List<String> strings(Object value) {
		List<String> out = new ArrayList<>();
		if (value instanceof List<?> list) {
			for (Object item : list) {
				if (item instanceof String s) {
					out.add(s);
				}
			}
		}
		return out;
	}
}
