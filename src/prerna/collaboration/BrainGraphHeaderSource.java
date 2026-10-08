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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;

// headers from Graph with the owner's delegated token, paged 100 at a time
final class BrainGraphHeaderSource implements BrainMailHeaderSource {

	private static final String BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";
	private static final String SELECT = "id,internetMessageId,conversationId,subject,from,toRecipients,ccRecipients,"
			+ "receivedDateTime,parentFolderId,sender,inferenceClassification";
	// Graph allows up to 1000 messages a page; headers only, so a page stays small
	private static final int PAGE = 500;
	private static final int BATCH = 20;
	private static final int LOOKUP_THREADS = 4;
	// Graph returns at most 50 chats or chat messages a page
	private static final int CHAT_PAGE = 50;
	// the subject of a chat with no topic of its own
	static final String CHAT_PREFIX = "Chat: ";
	private static final String USER_SELECT = "id,displayName,mail,userPrincipalName,jobTitle,department,companyName,"
			+ "accountEnabled,userType";
	private static final Logger classLogger = LogManager.getLogger(BrainGraphHeaderSource.class);

	@Override
	public Map<String, Object> me(User user) throws Exception {
		return get(user, BASE + "/me?$select=id,displayName,mail,userPrincipalName");
	}

	@Override
	public List<String> aliases(User user) {
		// "SMTP:primary@x" and "smtp:alias@x"; not every tenant returns them, so none
		// is not an error
		List<String> out = new ArrayList<>();
		try {
			if (get(user, BASE + "/me?$select=proxyAddresses").get("proxyAddresses") instanceof List<?> list) {
				for (Object value : list) {
					String v = String.valueOf(value);
					if (v.regionMatches(true, 0, "smtp:", 0, 5)) {
						out.add(v.substring(5).trim().toLowerCase());
					}
				}
			}
		} catch (Exception e) {
			return out;
		}
		return out;
	}

	@Override
	public Map<String, Object> manager(User user) {
		// no manager, or no directory permission: not an error for onboarding
		try {
			return get(user, BASE + "/me/manager?$select=id,displayName,mail,userPrincipalName");
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public List<Map<String, Object>> list(User user, String folder, Instant since, int max) throws Exception {
		return list(user, folder, since, max, SELECT);
	}

	@Override
	public List<Map<String, Object>> topicHeaders(User user, String folder, Instant since, int max) throws Exception {
		return list(user, folder, since, max, SELECT + ",internetMessageHeaders");
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> list(User user, String folder, Instant since, int max, String fields) throws Exception {
		List<Map<String, Object>> out = new ArrayList<>();
		String url = BASE + "/me/mailFolders/" + folder + "/messages?$select=" + fields + "&$top=" + PAGE
				+ "&$orderby=receivedDateTime%20desc&$filter=" + encode("receivedDateTime ge " + since);
		while (url != null && out.size() < max) {
			Map<String, Object> page = get(user, url);
			if (page.get("value") instanceof List<?> values) {
				for (Object value : values) {
					if (out.size() < max && value instanceof Map<?, ?> m) {
						out.add((Map<String, Object>) m);
					}
				}
			}
			url = (String) page.get("@odata.nextLink");
		}
		return out;
	}

	@Override
	public List<Map<String, Object>> chats(User user, Instant since, int maxChats, int maxPerChat) throws Exception {
		return importChats(user, since, maxChats, maxPerChat).messages();
	}

	@Override
	public ChatImport importChats(User user, Instant since, int maxChats, int maxPerChat) throws Exception {
		// one token for the run, so parallel calls do not race a refresh
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		String selfId = (String) get(token, BASE + "/me?$select=id").get("id");
		// newest activity first, so the first chat older than the window ends the list
		List<Map<String, Object>> chats = new ArrayList<>();
		String url = BASE + "/me/chats?$expand=lastMessagePreview&$top=" + CHAT_PAGE + "&$orderby="
				+ encode("lastMessagePreview/createdDateTime desc");
		list: while (url != null) {
			Map<String, Object> page = get(token, url);
			for (Map<String, Object> chat : values(page)) {
				Instant last = instant(
						chat.get("lastMessagePreview") instanceof Map<?, ?> p ? p.get("createdDateTime") : null);
				if (last == null) {
					continue;
				}
				if (last.isBefore(since) || chats.size() >= maxChats) {
					break list;
				}
				chats.add(chat);
			}
			url = (String) page.get("@odata.nextLink");
		}
		ExecutorService pool = Executors.newFixedThreadPool(LOOKUP_THREADS);
		try {
			List<Future<List<Map<String, Object>>>> calls = new ArrayList<>();
			for (Map<String, Object> chat : chats) {
				calls.add(pool.submit(() -> chatMessages(token, chat, selfId, since, maxPerChat)));
			}
			return collectChats(calls);
		} finally {
			pool.shutdownNow();
		}
	}

	// A failure anywhere in one chat (including members or later pages) leaves the other chats whole.
	static ChatImport collectChats(List<Future<List<Map<String, Object>>>> calls) throws InterruptedException {
		List<Map<String, Object>> out = new ArrayList<>();
		int skipped = 0;
		boolean reauthNeeded = false;
		for (Future<List<Map<String, Object>>> call : calls) {
			try {
				out.addAll(call.get());
			} catch (ExecutionException e) {
				Throwable cause = e.getCause() == null ? e : e.getCause();
				skipped++;
				reauthNeeded |= BrainMailHeaderSource.needsReauth(cause);
				classLogger.warn("Skipped a Teams chat for the import: {}", cause.getMessage());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			}
		}
		return new ChatImport(out, skipped, reauthNeeded);
	}

	// one chat's members, then its messages since the window opened, newest first
	private static List<Map<String, Object>> chatMessages(String token, Map<String, Object> chat, String selfId,
			Instant since, int max) throws Exception {
		String chatId = (String) chat.get("id");
		Map<String, Map<String, Object>> members = new LinkedHashMap<>();
		for (Map<String, Object> member : values(get(token, BASE + "/chats/" + encode(chatId) + "/members"))) {
			if (member.get("userId") instanceof String userId && member.get("email") instanceof String email
					&& !email.isBlank()) {
				members.put(userId, Map.of("emailAddress", Map.of("name", String.valueOf(member.get("displayName")),
						"address", email.trim().toLowerCase())));
			}
		}
		String subject = chatSubject(chat, members, selfId);
		List<Map<String, Object>> out = new ArrayList<>();
		// createdDateTime only filters with lt; lastModifiedDateTime takes gt with its
		// own order
		String url = BASE + "/chats/" + encode(chatId) + "/messages?$top=" + CHAT_PAGE + "&$orderby="
				+ encode("lastModifiedDateTime desc") + "&$filter="
				+ encode("lastModifiedDateTime gt " + since.truncatedTo(java.time.temporal.ChronoUnit.SECONDS));
		while (url != null && out.size() < max) {
			Map<String, Object> page = get(token, url);
			for (Map<String, Object> message : values(page)) {
				Map<String, Object> header = chatHeader(chatId, subject, members, message, since);
				if (header != null && out.size() < max) {
					header.put("chatType", chat.get("chatType"));
					out.add(header);
				}
			}
			url = (String) page.get("@odata.nextLink");
		}
		return out;
	}

	/**
	 * One chat message as a header, with members by user id in the recipient shape;
	 * null for system and app messages, deleted ones, ones before since, and
	 * senders without an address.
	 */
	@SuppressWarnings("unchecked")
	static Map<String, Object> chatHeader(String chatId, String subject, Map<String, Map<String, Object>> members,
			Map<String, Object> message, Instant since) {
		Instant at = instant(message.get("createdDateTime"));
		Object from = message.get("from") instanceof Map<?, ?> f ? f.get("user") : null;
		Map<String, Object> sender = from instanceof Map<?, ?> u ? members.get(((Map<String, Object>) u).get("id"))
				: null;
		Object type = message.get("messageType");
		if ((type != null && !"message".equals(type)) || message.get("deletedDateTime") != null || at == null
				|| at.isBefore(since) || sender == null) {
			return null;
		}
		List<Map<String, Object>> to = new ArrayList<>();
		for (Map<String, Object> member : members.values()) {
			if (member != sender) {
				to.add(member);
			}
		}
		Map<String, Object> header = new LinkedHashMap<>();
		header.put("id", message.get("id"));
		header.put("internetMessageId", "teams:" + chatId + ":" + message.get("id"));
		header.put("conversationId", chatId);
		header.put("subject", subject);
		header.put("from", sender);
		header.put("toRecipients", to);
		header.put("receivedDateTime", message.get("createdDateTime"));
		return header;
	}

	// the chat's own topic, or who else is in it (a 1:1 chat has none)
	@SuppressWarnings("unchecked")
	static String chatSubject(Map<String, Object> chat, Map<String, Map<String, Object>> members, String selfId) {
		if (chat.get("topic") instanceof String topic && !topic.isBlank()) {
			return topic.trim();
		}
		List<String> names = new ArrayList<>();
		members.forEach((id, member) -> {
			if (!id.equals(selfId)) {
				names.add(String.valueOf(((Map<String, Object>) member.get("emailAddress")).get("name")));
			}
		});
		String with = String.join(", ", names.subList(0, Math.min(4, names.size())));
		return CHAT_PREFIX + with + (names.size() > 4 ? " +" + (names.size() - 4) : "");
	}

	private static Instant instant(Object value) {
		try {
			return value instanceof String s && !s.isBlank() ? Instant.parse(s) : null;
		} catch (java.time.format.DateTimeParseException e) {
			return null;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> organization(User user) {
		// User.Read covers the signed-in user's own organisation
		try {
			Map<String, Object> page = get(user, BASE + "/organization?$select=displayName,verifiedDomains");
			if (page.get("value") instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof Map<?, ?> org) {
				List<String> domains = new ArrayList<>();
				if (org.get("verifiedDomains") instanceof List<?> verified) {
					for (Object v : verified) {
						if (v instanceof Map<?, ?> d && d.get("name") instanceof String name) {
							domains.add(name.toLowerCase());
						}
					}
				}
				Map<String, Object> out = new LinkedHashMap<>();
				out.put("name", ((Map<String, Object>) org).get("displayName"));
				out.put("domains", domains);
				return out;
			}
		} catch (Exception e) {
			classLogger.warn("Could not read the organization: {}", e.getMessage());
		}
		return Map.of();
	}

	@Override
	public Directory lookup(User user, List<String> addresses) {
		Map<String, Map<String, Object>> out = new ConcurrentHashMap<>();
		try {
			// people first (User.Read.All), then lists (GroupMember.Read.All) for what is
			// left
			run(user, addresses,
					a -> "/users?$filter="
							+ encode("mail eq '" + quote(a) + "' or userPrincipalName eq '" + quote(a) + "'")
							+ "&$select=" + USER_SELECT,
					(a, row) -> out.put(a, person(row)));
			List<String> rest = addresses.stream().filter(a -> !out.containsKey(a)).toList();
			run(user, rest,
					a -> "/groups?$filter=" + encode("mail eq '" + quote(a) + "'") + "&$select=id,displayName,mail",
					(a, row) -> out.put(a, entry("list", row, "Distribution list")));
		} catch (Exception e) {
			// a missing scope or a throttled tenant: keep what came back, the rules cover
			// the rest
			classLogger.warn("Directory lookup stopped after {} of {}: {}", out.size(), addresses.size(),
					e.getMessage());
			return new Directory(out, false);
		}
		return new Directory(out, true);
	}

	@Override
	public List<Map<String, Object>> searchDirectory(User user, String query, int max) {
		// $search matches word prefixes in any order, so "ryan weiler" finds "Weiler, Ryan"
		String text = query.replace("\"", " ").replace("\\", " ").trim();
		String field = text.contains("@") ? "mail" : "displayName";
		String url = BASE + "/users?$search=" + encode("\"" + field + ":" + text + "\"") + "&$select=" + USER_SELECT
				+ "&$top=" + Math.min(max * 2, 50);
		List<Map<String, Object>> out = new ArrayList<>();
		try {
			Map<String, String> headers = MicrosoftLoginUtils
					.getBearerHeader(MicrosoftLoginUtils.getValidAccessToken(user));
			// $search on users needs eventual consistency
			headers.put("ConsistencyLevel", "eventual");
			for (Map<String, Object> row : values(
					CollaborationDbUtils.parseMap(HttpHelperUtility.getRequest(url, headers, null, null, null)))) {
				String email = address(row);
				// admin and service accounts have no mailbox
				if (email == null || row.get("mail") == null || Boolean.FALSE.equals(row.get("accountEnabled"))) {
					continue;
				}
				Map<String, Object> person = new LinkedHashMap<>();
				person.put("name", row.get("displayName"));
				person.put("email", email);
				person.put("title", row.get("jobTitle"));
				person.put("department", row.get("department"));
				person.put("guest", "Guest".equalsIgnoreCase(String.valueOf(row.get("userType"))));
				out.add(person);
				if (out.size() >= max) {
					break;
				}
			}
		} catch (Exception e) {
			classLogger.warn("Directory search failed: {}", e.getMessage());
		}
		return out;
	}

	@Override
	public Map<String, String> orgChart(User user, String managerId) {
		Map<String, String> out = new LinkedHashMap<>();
		try {
			if (managerId != null) {
				for (Map<String, Object> row : values(get(user, BASE + "/users/" + encode(managerId)
						+ "/directReports?$select=mail,userPrincipalName&$top=100"))) {
					String a = address(row);
					if (a != null) {
						out.put(a, "peer");
					}
				}
			}
			for (Map<String, Object> row : values(
					get(user, BASE + "/me/directReports?$select=mail,userPrincipalName&$top=100"))) {
				String a = address(row);
				if (a != null) {
					out.put(a, "report");
				}
			}
		} catch (Exception e) {
			classLogger.warn("Could not read the org chart: {}", e.getMessage());
		}
		return out;
	}

	// person, guest, or a mailbox with no one signing in to it (shared, room, or a
	// system sender)
	private static Map<String, Object> person(Map<String, Object> row) {
		if (Boolean.FALSE.equals(row.get("accountEnabled"))) {
			return entry("mailbox", row, "Shared mailbox");
		}
		return entry("Guest".equalsIgnoreCase(String.valueOf(row.get("userType"))) ? "guest" : "person", row, null);
	}

	private static Map<String, Object> entry(String kind, Map<String, Object> row, String title) {
		Map<String, Object> e = new LinkedHashMap<>();
		e.put("kind", kind);
		e.put("id", row.get("id"));
		e.put("name", row.get("displayName"));
		e.put("title", title != null ? title : row.get("jobTitle"));
		e.put("department", row.get("department"));
		e.put("company", row.get("companyName"));
		return e;
	}

	@FunctionalInterface
	private interface Found {
		void accept(String address, Map<String, Object> row);
	}

	// $batch, 20 requests a call, a few calls at a time; a 403 stops the run (no
	// scope), other misses are skipped
	@SuppressWarnings("unchecked")
	private static void run(User user, List<String> addresses, java.util.function.Function<String, String> url,
			Found found) throws Exception {
		if (addresses.isEmpty()) {
			return;
		}
		// one token for the run, so parallel calls do not race a refresh
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		ExecutorService pool = Executors.newFixedThreadPool(LOOKUP_THREADS);
		try {
			List<Future<?>> calls = new ArrayList<>();
			for (int start = 0; start < addresses.size(); start += BATCH) {
				List<String> part = addresses.subList(start, Math.min(addresses.size(), start + BATCH));
				calls.add(pool.submit(() -> {
					List<Map<String, Object>> requests = new ArrayList<>();
					for (int i = 0; i < part.size(); i++) {
						requests.add(Map.of("id", String.valueOf(i), "method", "GET", "url", url.apply(part.get(i))));
					}
					Map<String, Object> reply = CollaborationDbUtils.parseMap(HttpHelperUtility.postRequestStringBody(
							BASE + "/$batch", MicrosoftLoginUtils.getBearerHeader(token),
							CollaborationDbUtils.toJson(Map.of("requests", requests)), ContentType.APPLICATION_JSON,
							null, null, null));
					for (Object r : reply.get("responses") instanceof List<?> list ? list : List.of()) {
						Map<String, Object> response = (Map<String, Object>) r;
						int status = ((Number) response.getOrDefault("status", 0)).intValue();
						if (status == 403) {
							throw new IllegalStateException("The directory is not readable with this sign-in (403)");
						}
						if (status == 200 && response.get("body") instanceof Map<?, ?> body) {
							List<Map<String, Object>> rows = values((Map<String, Object>) body);
							if (!rows.isEmpty()) {
								found.accept(part.get(Integer.parseInt(String.valueOf(response.get("id")))),
										rows.get(0));
							}
						}
					}
					return null;
				}));
			}
			for (Future<?> call : calls) {
				try {
					call.get();
				} catch (ExecutionException e) {
					throw e.getCause() instanceof Exception cause ? cause : e;
				}
			}
		} finally {
			pool.shutdownNow();
		}
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> values(Map<String, Object> page) {
		List<Map<String, Object>> out = new ArrayList<>();
		if (page.get("value") instanceof List<?> list) {
			for (Object v : list) {
				if (v instanceof Map<?, ?> m) {
					out.add((Map<String, Object>) m);
				}
			}
		}
		return out;
	}

	private static String address(Map<String, Object> row) {
		Object a = row.get("mail") != null ? row.get("mail") : row.get("userPrincipalName");
		return a == null ? null : String.valueOf(a).trim().toLowerCase();
	}

	// OData string literal: a quote is doubled
	private static String quote(String value) {
		return value.replace("'", "''");
	}

	private static Map<String, Object> get(User user, String url) throws Exception {
		return get(MicrosoftLoginUtils.getValidAccessToken(user), url);
	}

	private static Map<String, Object> get(String token, String url) throws Exception {
		return CollaborationDbUtils.parseMap(
				HttpHelperUtility.getRequest(url, MicrosoftLoginUtils.getBearerHeader(token), null, null, null));
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}
}
