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
package prerna.io.connector.google.gmail;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.google.GoogleLoginUtils;
import prerna.security.HttpHelperUtility;

/**
 * The Gmail API, as plain calls for one signed in user.
 *
 * <p>
 * What the methods return is Gmail's own json, parsed into maps.
 * {@link GoogleGmailMessageMapper} turns that into the records every mail
 * reactor answers with, and {@link GoogleGmailMime} writes the messages these
 * calls send. Gmail answers an error with a status the http helper throws on,
 * so nothing here has to look for one.
 * </p>
 */
public final class GoogleGmailHelper {

	private static final Logger classLogger = LogManager.getLogger(GoogleGmailHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String BASE = "https://gmail.googleapis.com/gmail/v1/users/me";
	private static final String GOOGLE_GMAIL_PROFILE_URL = BASE + "/profile";

	/** The most ids Gmail lists on one page. */
	private static final int MAX_PAGE = 500;

	private final String accessToken;

	/**
	 * @param accessToken the signed in user's Google access token
	 */
	public GoogleGmailHelper(String accessToken) {
		this.accessToken = accessToken;
	}

	/**
	 * One page of message ids, and whether Gmail has more.
	 *
	 * @param ids     the ids, newest first
	 * @param hasMore whether there are more after them
	 */
	public record IdPage(List<String> ids, boolean hasMore) {

	}

	/**
	 * List the ids of the messages that match, newest first, reading page after
	 * page until there are enough.
	 *
	 * @param query   a Gmail search, such as {@code is:unread}, or null
	 * @param labelId the label every message has to carry, or null for any
	 * @param needed  how many ids to read, counted from the newest
	 * @return the ids
	 */
	public IdPage listMessageIds(String query, String labelId, int needed) {
		List<String> ids = new ArrayList<>();
		String pageToken = null;
		do {
			StringBuilder url = new StringBuilder(BASE).append("/messages?maxResults=")
					.append(Math.min(MAX_PAGE, Math.max(1, needed - ids.size())));
			if (query != null && !query.isBlank()) {
				url.append("&q=").append(encode(query));
			}
			if (labelId != null) {
				url.append("&labelIds=").append(encode(labelId));
			}
			if (pageToken != null) {
				url.append("&pageToken=").append(encode(pageToken));
			}
			Map<String, Object> page = get(url.toString());
			for (Map<String, Object> message : listOf(page, "messages")) {
				if (message.get("id") != null) {
					ids.add(message.get("id").toString());
				}
			}
			pageToken = page == null || page.get("nextPageToken") == null ? null : page.get("nextPageToken").toString();
		} while (pageToken != null && ids.size() < needed);
		return new IdPage(ids.size() > needed ? ids.subList(0, needed) : ids, pageToken != null || ids.size() > needed);
	}

	/**
	 * @param id the message
	 * @return the message, with its headers and every part
	 */
	public Map<String, Object> getMessage(String id) {
		return get(BASE + "/messages/" + encode(id) + "?format=full");
	}

	/**
	 * @param id the message
	 * @return the message's labels and thread, without its content
	 */
	public Map<String, Object> getMessageLabels(String id) {
		return get(BASE + "/messages/" + encode(id) + "?format=minimal");
	}

	/**
	 * @param id the thread
	 * @return the thread, with every message in full, oldest first
	 */
	public Map<String, Object> getThread(String id) {
		return get(BASE + "/threads/" + encode(id) + "?format=full");
	}

	/**
	 * @param messageId    the message
	 * @param attachmentId the attachment, as the message's part names it
	 * @return the attachment, with its bytes as base64url {@code data}
	 */
	public Map<String, Object> getAttachment(String messageId, String attachmentId) {
		return get(BASE + "/messages/" + encode(messageId) + "/attachments/" + encode(attachmentId));
	}

	/**
	 * Send a message.
	 *
	 * @param raw      the message, as {@link GoogleGmailMime} writes it
	 * @param threadId the thread it answers, or null
	 * @return the message as sent, with its id and thread
	 */
	public Map<String, Object> send(String raw, String threadId) {
		return post(BASE + "/messages/send", message(raw, threadId));
	}

	/**
	 * Save a message as a draft.
	 *
	 * @param raw      the message, as {@link GoogleGmailMime} writes it
	 * @param threadId the thread it answers, or null
	 * @return the draft, with its own id and the id of the message it holds
	 */
	public Map<String, Object> createDraft(String raw, String threadId) {
		return post(BASE + "/drafts", Map.of("message", message(raw, threadId)));
	}

	/**
	 * @param draftId the draft
	 * @return the draft, with the message it holds in full
	 */
	public Map<String, Object> getDraft(String draftId) {
		return get(BASE + "/drafts/" + encode(draftId) + "?format=full");
	}

	/**
	 * Find the draft holding a message, since a listing of the drafts folder names
	 * messages and a draft is sent by its own id.
	 *
	 * @param messageId the message
	 * @return the draft's id, or null when no draft holds it
	 */
	public String findDraftId(String messageId) {
		String pageToken = null;
		do {
			String url = BASE + "/drafts?maxResults=" + MAX_PAGE
					+ (pageToken == null ? "" : "&pageToken=" + encode(pageToken));
			Map<String, Object> page = get(url);
			for (Map<String, Object> draft : listOf(page, "drafts")) {
				if (draft.get("message") instanceof Map<?, ?> message && messageId.equals(message.get("id"))) {
					return String.valueOf(draft.get("id"));
				}
			}
			pageToken = page == null || page.get("nextPageToken") == null ? null : page.get("nextPageToken").toString();
		} while (pageToken != null);
		return null;
	}

	/**
	 * Send a saved draft.
	 *
	 * @param draftId the draft
	 * @return the message as sent, with its id and thread
	 */
	public Map<String, Object> sendDraft(String draftId) {
		return post(BASE + "/drafts/send", Map.of("id", draftId));
	}

	/**
	 * Change a message's labels, which is how Gmail files, moves and marks mail.
	 *
	 * @param id     the message
	 * @param add    the labels to add
	 * @param remove the labels to take off
	 */
	public void modify(String id, List<String> add, List<String> remove) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("addLabelIds", add == null ? List.of() : add);
		body.put("removeLabelIds", remove == null ? List.of() : remove);
		post(BASE + "/messages/" + encode(id) + "/modify", body);
	}

	/**
	 * Move a message to the trash, from where it can still be restored.
	 *
	 * @param id the message
	 */
	public void trash(String id) {
		post(BASE + "/messages/" + encode(id) + "/trash", Map.of());
	}

	/**
	 * Take a message back out of the trash.
	 *
	 * @param id the message
	 */
	public void untrash(String id) {
		post(BASE + "/messages/" + encode(id) + "/untrash", Map.of());
	}

	/**
	 * @return every label of the mailbox, the ones Gmail keeps and the ones the
	 *         user made
	 */
	public List<Map<String, Object>> listLabels() {
		return listOf(get(BASE + "/labels"), "labels");
	}

	/**
	 * @param id the label
	 * @return the label, with how many messages and threads carry it
	 */
	public Map<String, Object> getLabel(String id) {
		return get(BASE + "/labels/" + encode(id));
	}

	/**
	 * Retrieves profile metadata for the logged-in Gmail user.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @return profile map including email address, message/thread counts, and
	 *         history ID.
	 * @throws Exception if the profile request fails.
	 */
	public static Map<String, Object> getGmailProfileById(String accessToken) throws Exception {
		try {
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(GOOGLE_GMAIL_PROFILE_URL, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("emailAddress", json.get("emailAddress"));
			map.put("messagesTotal", json.get("messagesTotal"));
			map.put("threadsTotal", json.get("threadsTotal"));
			map.put("historyId", json.get("historyId"));
			return map;
		} catch (Exception e) {
			classLogger.error("Failed to fetch Gmail profile", e);
			throw e;
		}
	}

	private static Map<String, Object> message(String raw, String threadId) {
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("raw", raw);
		if (threadId != null) {
			message.put("threadId", threadId);
		}
		return message;
	}

	private Map<String, Object> get(String url) {
		return readMap(HttpHelperUtility.getRequest(url, GoogleLoginUtils.getBearerHeader(this.accessToken), null, null,
				null));
	}

	private Map<String, Object> post(String url, Object body) {
		return readMap(HttpHelperUtility.postRequestStringBody(url, GoogleLoginUtils.getBearerHeader(this.accessToken),
				GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null));
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> listOf(Map<String, Object> response, String key) {
		if (response == null || !(response.get(key) instanceof List)) {
			return List.of();
		}
		return (List<Map<String, Object>>) response.get(key);
	}

	private static Map<String, Object> readMap(String response) {
		if (response == null || response.trim().isEmpty()) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}
}
