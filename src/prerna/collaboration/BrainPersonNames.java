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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// Finds the owner's contacts for names the owner typed ("first name", "first and last initial", "Last, First").
// A lookup the owner directs, not a classification; a name that fits several people stays a choice for the owner.
final class BrainPersonNames {

	// a match must beat the next one by this much to be picked without asking
	private static final double MARGIN = 1.0;
	private static final int OPTIONS = 4;
	private static final int LOOKUP = 40;

	record Person(String id, String name, String title, double score) {
	}

	/** One typed name: the person picked, or the options to ask about, or neither when nobody fits. */
	record Resolved(String typed, Person picked, List<Person> options) {
	}

	private BrainPersonNames() {
	}

	/** Resolve names typed together (one topic's people); people already settled break ties for the rest. */
	static List<Resolved> resolve(String ownerId, String ownerType, List<String> typed, Collection<String> peers) {
		List<List<Person>> found = new ArrayList<>();
		List<String> settled = new ArrayList<>(peers);
		for (String name : typed) {
			List<Person> matches = find(ownerId, ownerType, name);
			found.add(matches);
			if (clear(matches)) {
				settled.add(matches.get(0).id());
			}
		}
		List<Resolved> out = new ArrayList<>();
		for (int i = 0; i < typed.size(); i++) {
			List<Person> matches = found.get(i);
			if (matches.isEmpty()) {
				// "first x" where no x fits: the people with that first name are offered, never picked
				List<String> words = words(typed.get(i));
				List<Person> loose = words.size() > 1 ? find(ownerId, ownerType, words.get(0)) : List.of();
				out.add(new Resolved(typed.get(i), null, loose.stream().limit(OPTIONS).toList()));
				continue;
			}
			if (clear(matches)) {
				out.add(new Resolved(typed.get(i), matches.get(0), List.of()));
				continue;
			}
			// the one of several who shares conversations with the topic's other people
			List<Person> close = matches.stream().filter(p -> p.score() >= matches.get(0).score() - MARGIN)
					.limit(OPTIONS).toList();
			Map<String, Integer> shared = shared(ownerId, ownerType, close.stream().map(Person::id).toList(), settled);
			List<Person> byShared = close.stream()
					.sorted(Comparator.comparingInt((Person p) -> shared.getOrDefault(p.id(), 0)).reversed()).toList();
			int best = shared.getOrDefault(byShared.get(0).id(), 0);
			int next = byShared.size() > 1 ? shared.getOrDefault(byShared.get(1).id(), 0) : 0;
			if (best > 0 && best >= 2 * next + 1) {
				out.add(new Resolved(typed.get(i), byShared.get(0), List.of()));
			} else {
				out.add(new Resolved(typed.get(i), null, close));
			}
		}
		return out;
	}

	private static boolean clear(List<Person> matches) {
		return !matches.isEmpty() && (matches.size() == 1 || matches.get(0).score() - matches.get(1).score() >= MARGIN);
	}

	// best first; a word that contradicts the typed name rules the person out
	static List<Person> find(String ownerId, String ownerType, String typed) {
		List<String> words = words(typed);
		if (words.isEmpty() || words.get(0).length() < 2) {
			return List.of();
		}
		String first = words.get(0);
		List<Person> out = new ArrayList<>();
		CollaborationDbUtils.query(CollaborationDbUtils.page("SELECT PERSON_ID, DISPLAY_NAME, EMAIL_NORM, JOB_TITLE, "
				+ "STRENGTH, IS_VIP FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND COALESCE(RELATIONSHIP, '') NOT IN (?, ?) AND (LOWER(DISPLAY_NAME) LIKE ? OR LOWER(DISPLAY_NAME) LIKE ? "
				+ "OR LOWER(DISPLAY_NAME) LIKE ? OR EMAIL_NORM LIKE ?) ORDER BY COALESCE(STRENGTH, 0) DESC, PERSON_ID",
				LOOKUP, 0), rs -> {
					String display = Objects.toString(rs.getString("DISPLAY_NAME"), "");
					String email = Objects.toString(rs.getString("EMAIL_NORM"), "");
					List<String> tokens = new ArrayList<>(words(display));
					String local = email.contains("@") ? email.substring(0, email.indexOf('@')) : email;
					for (String part : local.split("[._\\-]+")) {
						if (!part.isBlank()) {
							tokens.add(part);
						}
					}
					double score = score(words, tokens);
					if (score > 0) {
						score += Math.min(100, rs.getInt("STRENGTH")) / 100.0
								+ (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_VIP")) ? 0.3 : 0);
						out.add(new Person(rs.getString("PERSON_ID"), display.isBlank() ? email : display,
								Objects.toString(rs.getString("JOB_TITLE"), ""), score));
					}
					return null;
				}, ownerId, ownerType, BrainSenderTyping.AUTOMATED, "self", first + "%", "% " + first + "%",
				"%," + first + "%", first + "%");
		out.sort(Comparator.comparingDouble(Person::score).reversed());
		return out;
	}

	// first word: whole word 3, start of a word 2; each later word or initial adds, or rules the person out
	private static double score(List<String> typed, List<String> tokens) {
		String first = typed.get(0);
		double score = tokens.contains(first) ? 3 : tokens.stream().anyMatch(t -> t.startsWith(first)) ? 2 : 0;
		if (score == 0) {
			return 0;
		}
		for (String word : typed.subList(1, typed.size())) {
			boolean whole = tokens.contains(word);
			boolean start = tokens.stream().filter(t -> !t.startsWith(first) || t.equals(word)).anyMatch(t -> t.startsWith(word));
			if (word.length() == 1 ? start : whole) {
				score += word.length() == 1 ? 1 : 2;
			} else if (word.length() > 1 && start) {
				score += 1;
			} else {
				return 0;
			}
		}
		return score;
	}

	private static List<String> words(String text) {
		List<String> out = new ArrayList<>();
		for (String word : Objects.toString(text, "").toLowerCase(Locale.ROOT).split("[^\\p{L}']+")) {
			if (!word.isBlank()) {
				out.add(word.replace("'", ""));
			}
		}
		return out;
	}

	// conversations each option shares with any of the peers
	private static Map<String, Integer> shared(String ownerId, String ownerType, List<String> options,
			List<String> peers) {
		Map<String, Integer> out = new HashMap<>();
		List<String> others = peers.stream().filter(p -> !options.contains(p)).distinct().limit(60).toList();
		if (options.isEmpty() || others.isEmpty()) {
			return out;
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(options);
		params.addAll(others);
		CollaborationDbUtils.query("SELECT a.PERSON_ID, COUNT(DISTINCT a.THREAD_ID) FROM BRAIN_THREAD_PARTICIPANT a "
				+ "JOIN BRAIN_THREAD_PARTICIPANT b ON b.OWNER_ID = a.OWNER_ID AND b.OWNER_TYPE = a.OWNER_TYPE "
				+ "AND b.THREAD_ID = a.THREAD_ID WHERE a.OWNER_ID = ? AND a.OWNER_TYPE = ? AND a.PERSON_ID IN ("
				+ CollaborationDbUtils.placeholders(options.size()) + ") AND b.PERSON_ID IN ("
				+ CollaborationDbUtils.placeholders(others.size()) + ") GROUP BY a.PERSON_ID", rs -> {
					out.put(rs.getString(1), rs.getInt(2));
					return null;
				}, params.toArray());
		return out;
	}

	/** Resolutions as change fields: people picked, names to choose between, names nobody fits. */
	static Map<String, Object> fields(List<Resolved> resolved, Set<String> already) {
		Map<String, Map<String, Object>> picked = new LinkedHashMap<>();
		List<Map<String, Object>> choices = new ArrayList<>();
		List<String> unknown = new ArrayList<>();
		for (Resolved r : resolved) {
			if (r.picked() != null) {
				if (!already.contains(r.picked().id())) {
					picked.putIfAbsent(r.picked().id(), Map.of("id", r.picked().id(), "name", r.picked().name()));
				}
			} else if (!r.options().isEmpty()) {
				choices.add(Map.of("name", r.typed(), "options", r.options().stream()
						.map(p -> Map.of("id", p.id(), "name", p.name(), "title", p.title())).toList()));
			} else {
				unknown.add(r.typed());
			}
		}
		return Map.of("people", new ArrayList<>(picked.values()), "choices", choices, "unknownNames", unknown);
	}
}
