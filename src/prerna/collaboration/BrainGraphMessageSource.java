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

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;

// reads one message from Graph with the owner's delegated token, only after the rules let it through
final class BrainGraphMessageSource implements BrainMessageSource {

	private static final String BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";
	private static final int BATCH = 20;
	private static final int RETRIES = 3;
	private static final long MAX_WAIT_MS = 10000;
	private static final String MAIL_SELECT = "subject,from,toRecipients,ccRecipients,body,uniqueBody,receivedDateTime,"
			+ "conversationId,webLink,hasAttachments";
	// the base attachment properties only, so a listing never carries file bytes
	private static final String ATTACHMENT_SELECT = "id,name,contentType,size,isInline";

	@Override
	public Map<String, Object> fetch(User user, String source, String conversationId, String graphId) throws Exception {
		String path = path(source, conversationId, graphId);
		if (path == null) {
			return null;
		}
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		Map<String, Object> message = CollaborationDbUtils.parseMap(HttpHelperUtility.getRequest(BASE + path,
				MicrosoftLoginUtils.getBearerHeader(token), null, null, null));
		return "teams".equals(source) ? fromChat(message) : message;
	}

	// Graph $batch, 20 a call: one round trip instead of one per message
	@Override
	@SuppressWarnings("unchecked")
	public List<Fetched> fetchAll(User user, String source, String conversationId, List<String> graphIds) {
		Fetched[] out = new Fetched[graphIds.size()];
		String token;
		try {
			token = MicrosoftLoginUtils.getValidAccessToken(user);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			List<Fetched> failed = new ArrayList<>();
			for (int i = 0; i < graphIds.size(); i++) {
				failed.add(new Fetched(null, e));
			}
			return failed;
		}
		for (int start = 0; start < graphIds.size(); start += BATCH) {
			List<Map<String, Object>> requests = new ArrayList<>();
			for (int i = start; i < Math.min(graphIds.size(), start + BATCH); i++) {
				String path = path(source, conversationId, graphIds.get(i));
				if (path == null) {
					out[i] = new Fetched(null, null);
				} else {
					requests.add(Map.of("id", String.valueOf(i), "method", "GET", "url", path));
				}
			}
			if (requests.isEmpty()) {
				continue;
			}
			// a throttled (429) or busy (5xx) message is sent again after Graph's Retry-After
			for (int attempt = 0; !requests.isEmpty(); attempt++) {
				List<Object> responses;
				try {
					Map<String, Object> reply = CollaborationDbUtils.parseMap(HttpHelperUtility.postRequestStringBody(
							BASE + "/$batch", MicrosoftLoginUtils.getBearerHeader(token),
							CollaborationDbUtils.toJson(Map.of("requests", requests)), ContentType.APPLICATION_JSON, null,
							null, null));
					responses = reply.get("responses") instanceof List<?> list ? (List<Object>) list : List.of();
				} catch (Exception e) {
					for (Map<String, Object> request : requests) {
						out[Integer.parseInt((String) request.get("id"))] = new Fetched(null, e);
					}
					break;
				}
				List<Map<String, Object>> again = new ArrayList<>();
				long wait = 0;
				for (Object r : responses) {
					Map<String, Object> response = (Map<String, Object>) r;
					int i = Integer.parseInt(String.valueOf(response.get("id")));
					int status = response.get("status") instanceof Number n ? n.intValue() : 0;
					if (status == 200 && response.get("body") instanceof Map<?, ?> body) {
						Map<String, Object> message = (Map<String, Object>) body;
						out[i] = new Fetched("teams".equals(source) ? fromChat(message) : message, null);
					} else if (status == 404) {
						out[i] = new Fetched(null, null);
					} else if ((status == 429 || status >= 500) && attempt < RETRIES) {
						requests.stream().filter(q -> String.valueOf(i).equals(q.get("id"))).findFirst()
								.ifPresent(again::add);
						wait = Math.max(wait, retryAfter(response, attempt));
					} else {
						out[i] = new Fetched(null, new IllegalStateException("Graph returned " + status));
					}
				}
				requests = again;
				if (!requests.isEmpty()) {
					try {
						Thread.sleep(wait);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
		}
		List<Fetched> list = new ArrayList<>();
		for (Fetched f : out) {
			list.add(f == null ? new Fetched(null, new IllegalStateException("No reply from Graph")) : f);
		}
		return list;
	}

	// Graph's Retry-After in seconds when it sends one, else 1, 2, 4 s; never more than MAX_WAIT_MS
	@SuppressWarnings("unchecked")
	private static long retryAfter(Map<String, Object> response, int attempt) {
		long fallback = 1000L << attempt;
		if (response.get("headers") instanceof Map<?, ?> headers) {
			for (Map.Entry<?, ?> h : ((Map<Object, Object>) headers).entrySet()) {
				if ("retry-after".equalsIgnoreCase(String.valueOf(h.getKey()))) {
					try {
						return Math.min(MAX_WAIT_MS, Long.parseLong(String.valueOf(h.getValue()).trim()) * 1000);
					} catch (NumberFormatException e) {
						return fallback;
					}
				}
			}
		}
		return Math.min(MAX_WAIT_MS, fallback);
	}

	// one $batch of attachment listings per 20 messages; a message Graph cannot list is left out
	@Override
	@SuppressWarnings("unchecked")
	public Map<String, List<Map<String, Object>>> attachments(User user, String source, List<String> graphIds)
			throws Exception {
		Map<String, List<Map<String, Object>>> out = new LinkedHashMap<>();
		if (!"email".equals(source) || graphIds.isEmpty()) {
			return out;
		}
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		for (int start = 0; start < graphIds.size(); start += BATCH) {
			List<Map<String, Object>> requests = new ArrayList<>();
			for (int i = start; i < Math.min(graphIds.size(), start + BATCH); i++) {
				requests.add(Map.of("id", String.valueOf(i), "method", "GET", "url",
						"/me/messages/" + encode(graphIds.get(i)) + "/attachments?$select=" + ATTACHMENT_SELECT));
			}
			Map<String, Object> reply = CollaborationDbUtils.parseMap(HttpHelperUtility.postRequestStringBody(
					BASE + "/$batch", MicrosoftLoginUtils.getBearerHeader(token),
					CollaborationDbUtils.toJson(Map.of("requests", requests)), ContentType.APPLICATION_JSON, null, null,
					null));
			List<Object> responses = reply.get("responses") instanceof List<?> list ? (List<Object>) list : List.of();
			for (Object r : responses) {
				Map<String, Object> response = (Map<String, Object>) r;
				int status = response.get("status") instanceof Number n ? n.intValue() : 0;
				if (status != 200 || !(response.get("body") instanceof Map<?, ?> body)
						|| !(body.get("value") instanceof List<?> items)) {
					continue;
				}
				List<Map<String, Object>> described = new ArrayList<>();
				for (Object item : items) {
					if (item instanceof Map<?, ?> attachment) {
						Map<String, Object> entry = BrainAttachments.describe((Map<String, Object>) attachment);
						if (!Boolean.TRUE.equals(entry.get("isInline"))) {
							described.add(entry);
						}
					}
				}
				out.put(graphIds.get(Integer.parseInt(String.valueOf(response.get("id")))), described);
			}
		}
		return out;
	}

	@Override
	public Map<String, Object> attachment(User user, String source, String graphId, String attachmentId)
			throws Exception {
		if (!"email".equals(source) || graphId == null || attachmentId == null) {
			return null;
		}
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		String url = BASE + attachmentPath(graphId, attachmentId) + "?$select=" + ATTACHMENT_SELECT;
		try {
			return CollaborationDbUtils.parseMap(
					HttpHelperUtility.getRequest(url, MicrosoftLoginUtils.getBearerHeader(token), null, null, null));
		} catch (IllegalArgumentException e) {
			// HttpHelperUtility reports a status as "... returned HTTP <code>"
			if (String.valueOf(e.getMessage()).contains("returned HTTP 404")) {
				return null;
			}
			throw e;
		}
	}

	// streams the raw bytes ($value), so a file is never held as base64 in memory;
	// HttpHelperUtility runs the platform virus scan when that is on
	@Override
	public Path download(User user, String source, String graphId, String attachmentId, Path dir, String fileName)
			throws Exception {
		if (!"email".equals(source)) {
			throw new IllegalArgumentException("Only email attachments can be read");
		}
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		File file = HttpHelperUtility.getRequestFileDownload(BASE + attachmentPath(graphId, attachmentId) + "/$value",
				MicrosoftLoginUtils.getBearerHeader(token), null, null, null, dir.toString(), fileName);
		return file.toPath();
	}

	private static String attachmentPath(String graphId, String attachmentId) {
		return "/me/messages/" + encode(graphId) + "/attachments/" + encode(attachmentId);
	}

	// the message's path under the Graph base, or null when the source cannot be
	// read
	private static String path(String source, String conversationId, String graphId) {
		if (graphId == null) {
			return null;
		}
		if ("email".equals(source)) {
			return "/me/messages/" + encode(graphId) + "?$select=" + MAIL_SELECT;
		}
		if ("teams".equals(source) && conversationId != null) {
			return "/chats/" + encode(conversationId) + "/messages/" + encode(graphId);
		}
		return null;
	}

	// a chat message has no subject, uniqueBody, or sender address; map it onto the
	// mail shape
	@SuppressWarnings("unchecked")
	static Map<String, Object> fromChat(Map<String, Object> chat) {
		if (chat == null) {
			return null;
		}
		Map<String, Object> from = chat.get("from") instanceof Map<?, ?> f ? (Map<String, Object>) f : Map.of();
		Map<String, Object> user = from.get("user") instanceof Map<?, ?> u ? (Map<String, Object>) u : Map.of();
		Map<String, Object> sender = new LinkedHashMap<>();
		sender.put("name", user.get("displayName"));
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("from", Map.of("emailAddress", sender));
		message.put("body", chat.get("body"));
		message.put("attachments", chat.get("attachments"));
		message.put("receivedDateTime", chat.get("createdDateTime"));
		message.put("conversationId", chat.get("chatId"));
		message.put("webLink", chat.get("webUrl"));
		return message;
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}
}
