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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.reactor.agent.stream.AgentStreamItems;
import prerna.security.HttpHelperUtility;
import prerna.security.HttpStatusException;

/** A live, one-level chart read with the signed-in user's Microsoft permissions. */
public final class WorkOrgChart {
	private static final String BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";
	private static final String FIELDS = "id,displayName,jobTitle,department,mail";
	private static final String USER_FIELDS = FIELDS + ",userPrincipalName";
	private static final int CANDIDATES = 10;
	private static final int TEAM = 25;

	private WorkOrgChart() {
	}

	public static Map<String, Object> show(User user, String query, String personId) {
		String name = query == null ? "" : query.strip();
		String id = personId == null ? "" : personId.strip();
		if (name.isEmpty() == id.isEmpty()) {
			throw new IllegalArgumentException("Provide a name, email or me as query, or one directory personId.");
		}
		if (!id.isEmpty() && !isId(id)) {
			throw new IllegalArgumentException("personId must be a Microsoft directory user id from a chart.");
		}
		if (name.length() > 320 || name.codePoints().anyMatch(Character::isISOControl)) {
			throw new IllegalArgumentException("Enter a name or email of up to 320 characters.");
		}
		Map<String, String> headers;
		try {
			headers = MicrosoftLoginUtils.getBearerHeader(MicrosoftLoginUtils.getValidAccessToken(user));
		} catch (Exception e) {
			return status("unavailable", "Sign in to your work or school Microsoft account to read the org chart.");
		}
		headers.put("ConsistencyLevel", "eventual");
		return lookup(id.isEmpty() ? name : id, term -> BrainPeopleUtils.findPeople(user, term, CANDIDATES + 1, false),
				url -> CollaborationDbUtils.parseMap(HttpHelperUtility.getRequest(
						HttpHelperUtility.getSystemClient(null, null, null, null, BrainGraphHeaderSource.TIMEOUT),
						url, headers)));
	}

	static Map<String, Object> lookup(String query, Function<String, List<Map<String, Object>>> contacts,
			Function<String, Map<String, Object>> graph) {
		try {
			Map<String, Object> selected;
			if (query.equalsIgnoreCase("me") || query.equalsIgnoreCase("my team")) {
				selected = person(graph.apply(BASE + "/me?$select=" + USER_FIELDS));
			} else if (isId(query)) {
				selected = person(graph.apply(BASE + "/users/" + query + "?$select=" + USER_FIELDS));
			} else {
				boolean address = query.contains("@");
				String condition = address ? emailFilter(query) : "$search=" + BrainGraphHeaderSource.userSearch(query);
				Page page = people(graph, BASE + "/users?" + condition + "&$select=" + USER_FIELDS
						+ "&$top=" + (CANDIDATES + 1), CANDIDATES, null);
				if (page.status.equals("unavailable")) {
					return status("unavailable", "Microsoft 365 could not search the directory with this sign-in.");
				}
				List<Map<String, Object>> candidates = new ArrayList<>(page.people);
				if (!address) {
					Set<String> emails = new HashSet<>();
					for (Map<String, Object> match : candidates) {
						if (match.get("email") instanceof String email) {
							emails.add(email.toLowerCase(Locale.ROOT));
						}
					}
					for (Map<String, Object> contact : contacts.apply(query)) {
						if (contact.get("email") instanceof String email && email.contains("@") && email.length() <= 320
								&& emails.add(email.toLowerCase(Locale.ROOT))) {
							candidates.add(contactCandidate(contact));
						}
					}
				}
				if (candidates.size() > 1 || page.status.equals("partial") && !candidates.isEmpty()) {
					return ambiguous(candidates, page.status.equals("partial"));
				}
				if (candidates.isEmpty()) {
					return page.status.equals("partial")
							? status("unavailable", "The directory search could not be completed.") : notFound();
				}
				selected = candidates.get(0);
				if (selected.get("id") == null) {
					// A contact may use a different name locally, or have no account in this directory.
					return lookup((String) selected.get("email"), contacts, graph);
				}
			}
			return chart(selected, graph);
		} catch (HttpStatusException e) {
			return e.getStatusCode() == 404 ? notFound()
					: status("unavailable", "Microsoft 365 could not return this org chart with your sign-in.");
		} catch (RuntimeException e) {
			return status("unavailable", "The directory lookup could not be completed. Try again.");
		}
	}

	private static String emailFilter(String address) {
		String literal = "'" + address.replace("'", "''") + "'";
		return "$filter=" + BrainGraphHeaderSource.encode("mail eq " + literal + " or userPrincipalName eq " + literal);
	}

	private static Map<String, Object> chart(Map<String, Object> selected, Function<String, Map<String, Object>> graph) {
		String id = (String) selected.get("id");
		String path = BASE + "/users/" + id;
		Map<String, Object> manager = null;
		String managerStatus = "complete";
		try {
			manager = person(graph.apply(path + "/manager?$select=" + FIELDS));
		} catch (HttpStatusException e) {
			managerStatus = e.getStatusCode() == 404 ? "not_listed" : "unavailable";
		} catch (RuntimeException e) {
			managerStatus = "unavailable";
		}
		Page reports = reports(graph, id, id);
		Page peers = manager == null ? new Page(new ArrayList<>(), managerStatus)
				: Boolean.FALSE.equals(manager.get("navigable")) ? new Page(new ArrayList<>(), "unavailable")
						: reports(graph, (String) manager.get("id"), id);
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("status", "ok");
		result.put("person", selected);
		result.put("manager", manager);
		result.put("managerStatus", managerStatus);
		result.put("directReports", reports.people);
		result.put("directReportsStatus", reports.status);
		result.put("peers", peers.people);
		result.put("peersStatus", peers.status);
		// The live tool stream truncates plain text. Trim whole entries so the card always receives valid JSON.
		while (tooLarge(result)) {
			boolean trimReports = reports.people.size() >= peers.people.size();
			(trimReports ? reports.people : peers.people).removeLast();
			result.put(trimReports ? "directReportsStatus" : "peersStatus", "partial");
		}
		return result;
	}

	private static Page reports(Function<String, Map<String, Object>> graph, String managerId, String exclude) {
		return people(graph, BASE + "/users/" + managerId + "/directReports?$select=" + FIELDS,
				TEAM, exclude);
	}

	private record Page(List<Map<String, Object>> people, String status) { }

	private static Page people(Function<String, Map<String, Object>> graph, String url, int limit, String exclude) {
		List<Map<String, Object>> people = new ArrayList<>();
		Set<String> ids = new HashSet<>();
		Set<String> pages = new HashSet<>();
		try {
			do {
				// Never forward the delegated token outside the Graph endpoint, even through a returned nextLink.
				if (!url.startsWith(BASE + "/") || !pages.add(url) || pages.size() > limit + 1) {
					return new Page(people, "partial");
				}
				Map<String, Object> page = graph.apply(url);
				if (!(page.get("value") instanceof List<?> rows)) {
					throw new IllegalStateException("Expected a directory collection");
				}
				for (Object entry : rows) {
					if (!(entry instanceof Map<?, ?> row)) {
						throw new IllegalStateException("Expected a directory person");
					}
					Map<String, Object> person = person(row);
					String id = (String) person.get("id");
					if (id.equalsIgnoreCase(exclude) || !ids.add(id.toLowerCase(Locale.ROOT))) {
						continue;
					}
					if (people.size() == limit) {
						return new Page(people, "partial");
					}
					people.add(person);
				}
				url = (String) page.get("@odata.nextLink");
			} while (url != null);
			return new Page(people, "complete");
		} catch (RuntimeException e) {
			return new Page(people, pages.size() > 1 || !people.isEmpty() ? "partial" : "unavailable");
		}
	}

	private static Map<String, Object> person(Map<?, ?> row) {
		if (!(row.get("id") instanceof String id) || !isId(id)) {
			throw new IllegalArgumentException("Expected a Microsoft directory object id");
		}
		Map<String, Object> person = new LinkedHashMap<>();
		person.put("id", id);
		String email = row.get("mail") instanceof String mail && !mail.isBlank() ? mail
				: row.get("userPrincipalName") instanceof String upn ? upn : null;
		putFields(person, row.get("displayName"), row.get("jobTitle"), row.get("department"), email);
		if ("#microsoft.graph.orgContact".equals(row.get("@odata.type")) || "Name unavailable".equals(person.get("name"))) {
			person.put("navigable", false);
		}
		return person;
	}

	private static Map<String, Object> contactCandidate(Map<String, Object> row) {
		Map<String, Object> person = new LinkedHashMap<>();
		putFields(person, row.get("name"), row.get("title"), row.get("department"), (String) row.get("email"));
		return person;
	}

	private static void putFields(Map<String, Object> person, Object name, Object title, Object department, String email) {
		if (email != null && !email.isBlank() && email.length() <= 320) {
			person.put("email", email);
		}
		putText(person, "name", name, 160);
		person.putIfAbsent("name", person.getOrDefault("email", "Name unavailable"));
		putText(person, "title", title, 160);
		putText(person, "department", department, 120);
	}

	private static void putText(Map<String, Object> person, String key, Object value, int limit) {
		if (value instanceof String text && !text.isBlank()) {
			person.put(key, text.length() <= limit ? text : text.substring(0,
					Character.isHighSurrogate(text.charAt(limit - 1)) ? limit - 1 : limit) + "...");
		}
	}

	private static Map<String, Object> ambiguous(List<Map<String, Object>> found, boolean truncated) {
		List<Map<String, Object>> candidates = new ArrayList<>(found.subList(0, Math.min(found.size(), CANDIDATES)));
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("status", "ambiguous");
		result.put("message", "Choose a matching person or enter their exact work email address.");
		result.put("candidates", candidates);
		result.put("candidatesTruncated", truncated || found.size() > candidates.size());
		while (tooLarge(result)) {
			candidates.removeLast();
			result.put("candidatesTruncated", true);
		}
		return result;
	}

	private static boolean tooLarge(Map<String, Object> result) {
		return CollaborationDbUtils.toJson(result).length() > AgentStreamItems.MAX_TOOL_OUTPUT_CHARS;
	}

	private static boolean isId(String value) {
		return BrainPeopleUtils.USER_ID.matcher(value).matches();
	}

	private static Map<String, Object> notFound() {
		return status("not_found", "No matching user was found in this Microsoft 365 directory.");
	}

	private static Map<String, Object> status(String status, String message) {
		return Map.of("status", status, "message", message);
	}
}
