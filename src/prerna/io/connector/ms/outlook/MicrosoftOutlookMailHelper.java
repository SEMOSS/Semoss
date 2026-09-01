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
package prerna.io.connector.ms.outlook;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.security.HttpHelperUtility;

/**
 * The Outlook mail operations of Microsoft Graph, as plain calls.
 *
 * <p>
 * Every method takes the access token to use and the mailbox to work against,
 * and nothing here knows or cares where either came from. That is deliberate:
 * the same call serves an app-only caller holding a client credentials token
 * for a particular mailbox, and a delegated caller holding a signed in user's
 * token for their own. A null mailbox addresses {@code /me}, which is the
 * delegated shape, and a named one addresses {@code /users/{mailbox}}, which is
 * the app only shape.
 *
 * <p>
 * What the methods return is Graph's own json, parsed into maps, rather than a
 * type of this codebase's invention. A caller that wants a tidier shape maps it
 * on the way out, and one that wants a field this class never thought about can
 * still reach it.
 *
 * <p>
 * Two Graph rules are worth knowing, because they are the ones that shape
 * {@link MessageQuery}: {@code $search} and {@code $filter} cannot be combined
 * on messages, and {@code $orderby} is ignored while searching. So a query that
 * asks for text finds by search and narrows the rest in memory, and one that
 * does not filters and orders on the server.
 */
public class MicrosoftOutlookMailHelper {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookMailHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	/** Where Graph lives, unless a sovereign cloud says otherwise. */
	public static final String DEFAULT_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0";

	/** The fields a message listing asks for when the caller wants the body. */
	private static final String MESSAGE_FIELDS = "id,subject,from,toRecipients,ccRecipients,receivedDateTime,"
			+ "sentDateTime,isRead,hasAttachments,bodyPreview,body,internetMessageId";

	/** The same without the body, for a listing that only wants headers. */
	private static final String MESSAGE_FIELDS_NO_BODY = "id,subject,from,toRecipients,ccRecipients,"
			+ "receivedDateTime,sentDateTime,isRead,hasAttachments,bodyPreview,internetMessageId";

	// graph refuses a message over this size on the simple send, and the limit is
	// on the encoded form rather than the file, so it is checked after encoding
	private static final long MAX_SEND_BYTES = 4L * 1024L * 1024L;

	private final String graphBaseUrl;

	/**
	 * Talk to the public Graph endpoint.
	 */
	public MicrosoftOutlookMailHelper() {
		this(DEFAULT_GRAPH_BASE_URL);
	}

	/**
	 * @param graphBaseUrl where Graph lives, for a sovereign cloud or a test double
	 */
	public MicrosoftOutlookMailHelper(String graphBaseUrl) {
		this.graphBaseUrl = graphBaseUrl == null || graphBaseUrl.trim().isEmpty() ? DEFAULT_GRAPH_BASE_URL
				: graphBaseUrl.trim().replaceAll("/+$", "");
	}

	/**
	 * Send one message.
	 *
	 * @param accessToken     the token to send with
	 * @param mailbox         the mailbox to send as, or null for the signed in user
	 * @param message         the message, as built by
	 *                        {@link #buildMessage(String, String, boolean, String[], String[], String[], String, String, String[])}
	 * @param saveToSentItems whether a copy is kept in Sent Items
	 * @throws IOException when the message is too large for a single send
	 */
	public void sendMail(String accessToken, String mailbox, Map<String, Object> message, boolean saveToSentItems)
			throws IOException {
		Map<String, Object> request = new LinkedHashMap<>();
		request.put("message", message);
		request.put("saveToSentItems", saveToSentItems);

		String body = GSON.toJson(request);
		// measured on the bytes rather than the characters, since the limit is on
		// what goes over the wire and a body is not all ascii
		int requestBytes = body.getBytes(StandardCharsets.UTF_8).length;
		if (requestBytes > MAX_SEND_BYTES) {
			throw new IOException("The message is " + requestBytes + " bytes, over the " + MAX_SEND_BYTES
					+ " byte limit Graph accepts on a single send");
		}

		String url = userPath(mailbox) + "/sendMail";
		classLogger.info("Sending an email through {}", url);
		// a successful sendMail answers 202 with no body, so anything coming back is
		// either an error graph described or a transport problem
		String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken), body,
				ContentType.APPLICATION_JSON, null, null, null);
		throwOnError(response, "send an email");
	}

	/**
	 * Save a message as a draft rather than sending it.
	 *
	 * <p>
	 * The draft lands in the mailbox's Drafts folder, where whoever owns it can
	 * read it, change it and decide whether it goes out. Nothing is sent, and the
	 * message can be sent later with {@link #sendDraft(String, String, String)}.
	 *
	 * @param accessToken the token to write with
	 * @param mailbox     the mailbox to save into, or null for the signed in user
	 * @param message     the message, as built by
	 *                    {@link #buildMessage(String, String, boolean, String[], String[], String[], String, String, String[])}
	 * @return the draft as Graph created it, which carries the id it was given and
	 *         a webLink that opens it in Outlook
	 */
	public Map<String, Object> createDraft(String accessToken, String mailbox, Map<String, Object> message) {
		String url = userPath(mailbox) + "/messages";
		classLogger.info("Saving a draft email through {}", url);
		// unlike sendMail, this answers with the message it created, since the id it
		// was given is the only way to find it again
		String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken), GSON.toJson(message),
				ContentType.APPLICATION_JSON, null, null, null);
		throwOnError(response, "save a draft email");
		return readMap(response);
	}

	/**
	 * Send a message that was saved as a draft.
	 *
	 * @param accessToken the token to send with
	 * @param mailbox     the mailbox holding the draft, or null for the signed in
	 *                    user
	 * @param messageId   the draft to send
	 */
	public void sendDraft(String accessToken, String mailbox, String messageId) {
		String url = userPath(mailbox) + "/messages/" + encode(messageId) + "/send";
		classLogger.info("Sending a saved draft through {}", url);
		// answers 202 with no body, the same way sendMail does
		String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken), "",
				ContentType.APPLICATION_JSON, null, null, null);
		throwOnError(response, "send a saved draft");
	}

	/**
	 * Build a message in the shape Graph reads.
	 *
	 * <p>
	 * Pure assembly, so a caller can hold it, log it, or hand it to
	 * {@link #sendMail(String, String, Map, boolean)} later.
	 *
	 * @param subject     the subject line
	 * @param body        the body
	 * @param html        whether the body is html rather than plain text
	 * @param to          the to recipients, or null
	 * @param cc          the cc recipients, or null
	 * @param bcc         the bcc recipients, or null
	 * @param fromAddress the address to send as, or null to let Graph decide from
	 *                    the mailbox
	 * @param fromName    a display name for that address, or null
	 * @param attachments file paths to attach, or null
	 * @return the message
	 * @throws IOException when an attachment cannot be read
	 */
	public static Map<String, Object> buildMessage(String subject, String body, boolean html, String[] to, String[] cc,
			String[] bcc, String fromAddress, String fromName, String[] attachments) throws IOException {
		Map<String, Object> content = new LinkedHashMap<>();
		content.put("contentType", html ? "HTML" : "Text");
		content.put("content", body == null ? "" : body);

		Map<String, Object> message = new LinkedHashMap<>();
		message.put("subject", subject == null ? "" : subject);
		message.put("body", content);
		if (fromAddress != null) {
			Map<String, Object> sentAs = new LinkedHashMap<>();
			sentAs.put("address", fromAddress);
			if (fromName != null) {
				// the address is the mailbox either way, so this only puts a readable
				// name in front of it rather than sending as somebody else
				sentAs.put("name", fromName);
			}
			message.put("from", Map.of("emailAddress", sentAs));
		}
		putRecipients(message, "toRecipients", to);
		putRecipients(message, "ccRecipients", cc);
		putRecipients(message, "bccRecipients", bcc);

		if (attachments != null && attachments.length > 0) {
			List<Map<String, Object>> attached = new ArrayList<>();
			for (String path : attachments) {
				File file = new File(path);
				Map<String, Object> attachment = new LinkedHashMap<>();
				// the only attachment type the simple send takes inline
				attachment.put("@odata.type", "#microsoft.graph.fileAttachment");
				attachment.put("name", file.getName());
				attachment.put("contentBytes", Base64.getEncoder().encodeToString(Files.readAllBytes(file.toPath())));
				attached.add(attachment);
			}
			message.put("attachments", attached);
		}
		return message;
	}

	/**
	 * Find messages in a mailbox.
	 *
	 * @param accessToken the token to read with
	 * @param mailbox     the mailbox to read, or null for the signed in user
	 * @param query       what to look for
	 * @return the messages, newest first, as Graph returned them
	 */
	public List<Map<String, Object>> listMessages(String accessToken, String mailbox, MessageQuery query) {
		StringBuilder url = new StringBuilder(userPath(mailbox));
		if (query.folder != null && !query.folder.isEmpty()) {
			url.append("/mailFolders/").append(encode(query.folder));
		}
		url.append("/messages?$select=").append(query.includeBody ? MESSAGE_FIELDS : MESSAGE_FIELDS_NO_BODY);
		url.append("&$top=").append(Math.max(1, query.top));

		boolean searching = query.from != null || query.subject != null;
		if (searching) {
			// $search cannot be combined with $filter, and ordering is ignored while
			// searching, so the rest of the query is applied below in memory
			List<String> terms = new ArrayList<>();
			if (query.from != null) {
				terms.add("from:" + query.from);
			}
			if (query.subject != null) {
				terms.add("subject:" + query.subject);
			}
			url.append("&$search=").append(encode('"' + String.join(" AND ", terms) + '"'));
		} else {
			List<String> filters = new ArrayList<>();
			if (query.since != null) {
				filters.add("receivedDateTime ge " + DateTimeFormatter.ISO_INSTANT.format(query.since.toInstant()));
			}
			if (query.unreadOnly) {
				filters.add("isRead eq false");
			}
			if (!filters.isEmpty()) {
				url.append("&$filter=").append(encode(String.join(" and ", filters)));
			}
			url.append("&$orderby=").append(encode("receivedDateTime desc"));
		}

		String response = HttpHelperUtility.getRequest(url.toString(), headers(accessToken), null, null, null);
		throwOnError(response, "read the mailbox");
		List<Map<String, Object>> messages = readList(response);

		if (searching) {
			// what the server could not be asked for alongside the text search
			List<Map<String, Object>> narrowed = new ArrayList<>();
			for (Map<String, Object> message : messages) {
				if (query.unreadOnly && Boolean.TRUE.equals(message.get("isRead"))) {
					continue;
				}
				if (query.since != null && receivedBefore(message, query.since)) {
					continue;
				}
				narrowed.add(message);
			}
			messages = narrowed;
		}
		return messages;
	}

	/**
	 * Read one message.
	 *
	 * @param accessToken the token to read with
	 * @param mailbox     the mailbox, or null for the signed in user
	 * @param messageId   the message to read
	 * @return the message as Graph returned it
	 */
	public Map<String, Object> getMessage(String accessToken, String mailbox, String messageId) {
		String url = userPath(mailbox) + "/messages/" + encode(messageId) + "?$select=" + MESSAGE_FIELDS;
		String response = HttpHelperUtility.getRequest(url, headers(accessToken), null, null, null);
		throwOnError(response, "read a message");
		return readMap(response);
	}

	/**
	 * List the attachments of a message, each carrying its bytes when it is a file.
	 *
	 * @param accessToken the token to read with
	 * @param mailbox     the mailbox, or null for the signed in user
	 * @param messageId   the message whose attachments to list
	 * @return the attachments as Graph returned them
	 */
	public List<Map<String, Object>> listAttachments(String accessToken, String mailbox, String messageId) {
		String url = userPath(mailbox) + "/messages/" + encode(messageId) + "/attachments";
		String response = HttpHelperUtility.getRequest(url, headers(accessToken), null, null, null);
		throwOnError(response, "read the attachments of a message");
		return readList(response);
	}

	/**
	 * Mark a message read or unread.
	 *
	 * @param accessToken the token to write with
	 * @param mailbox     the mailbox, or null for the signed in user
	 * @param messageId   the message to mark
	 * @param read        true to mark read, false to mark unread
	 */
	public void setRead(String accessToken, String mailbox, String messageId, boolean read) {
		String url = userPath(mailbox) + "/messages/" + encode(messageId);
		String response = HttpHelperUtility.patchRequestStringBody(url, headers(accessToken),
				GSON.toJson(Map.of("isRead", read)), ContentType.APPLICATION_JSON, null, null, null);
		throwOnError(response, "mark a message");
	}

	/**
	 * Move a message to another folder.
	 *
	 * @param accessToken       the token to write with
	 * @param mailbox           the mailbox, or null for the signed in user
	 * @param messageId         the message to move
	 * @param destinationFolder the folder to move it to, by well known name or id
	 * @return the id of the moved message, which Graph reissues on a move
	 */
	public String moveMessage(String accessToken, String mailbox, String messageId, String destinationFolder) {
		String url = userPath(mailbox) + "/messages/" + encode(messageId) + "/move";
		String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken),
				GSON.toJson(Map.of("destinationId", destinationFolder)), ContentType.APPLICATION_JSON, null, null,
				null);
		throwOnError(response, "move a message");
		Map<String, Object> moved = readMap(response);
		// a move is a copy and a delete on the server, so the message that comes
		// back is not the one that went in
		return moved == null ? null : (String) moved.get("id");
	}

	/**
	 * Delete a message. It goes to Deleted Items rather than disappearing, which is
	 * the one thing Graph does more gently than the mail protocols.
	 *
	 * @param accessToken the token to write with
	 * @param mailbox     the mailbox, or null for the signed in user
	 * @param messageId   the message to delete
	 */
	public void deleteMessage(String accessToken, String mailbox, String messageId) {
		String url = userPath(mailbox) + "/messages/" + encode(messageId);
		String response = HttpHelperUtility.deleteRequestStringBody(url, headers(accessToken), null, null, null);
		throwOnError(response, "delete a message");
	}

	/**
	 * List the folders of a mailbox.
	 *
	 * @param accessToken the token to read with
	 * @param mailbox     the mailbox, or null for the signed in user
	 * @return the folders as Graph returned them
	 */
	public List<Map<String, Object>> listFolders(String accessToken, String mailbox) {
		String url = userPath(mailbox) + "/mailFolders?$top=100&$select=id,displayName,totalItemCount";
		String response = HttpHelperUtility.getRequest(url, headers(accessToken), null, null, null);
		throwOnError(response, "list the folders of the mailbox");
		return readList(response);
	}

	/**
	 * Find the id of a folder by the name a person would call it.
	 *
	 * <p>
	 * Graph takes a well known name such as {@code inbox} straight through, so this
	 * is only needed for a folder somebody made.
	 *
	 * @param accessToken the token to read with
	 * @param mailbox     the mailbox, or null for the signed in user
	 * @param displayName the folder name to look for
	 * @return the folder id, or null when the mailbox has no such folder
	 */
	public String resolveFolderId(String accessToken, String mailbox, String displayName) {
		for (Map<String, Object> folder : listFolders(accessToken, mailbox)) {
			if (displayName.equalsIgnoreCase(String.valueOf(folder.get("displayName")))) {
				return (String) folder.get("id");
			}
		}
		return null;
	}

	/**
	 * The part of a Graph url that says whose mailbox this is.
	 *
	 * @param mailbox the mailbox, or null for the signed in user
	 * @return the url up to the mailbox
	 */
	private String userPath(String mailbox) {
		if (mailbox == null || mailbox.trim().isEmpty()) {
			// the delegated shape, where the token already says who this is
			return this.graphBaseUrl + "/me";
		}
		return this.graphBaseUrl + "/users/" + encode(mailbox.trim());
	}

	/**
	 * @param accessToken the token to send
	 * @return the headers every call carries
	 */
	private static Map<String, String> headers(String accessToken) {
		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", "Bearer " + accessToken);
		return headers;
	}

	/**
	 * Turn an error Graph described into an exception, so a caller does not have to
	 * inspect every response.
	 *
	 * @param response what came back
	 * @param what     what was being attempted, for the message
	 */
	private static void throwOnError(String response, String what) {
		if (response == null || !response.contains("\"error\"")) {
			return;
		}
		String detail = response;
		try {
			Map<String, Object> parsed = readMap(response);
			Object error = parsed == null ? null : parsed.get("error");
			if (error instanceof Map) {
				Map<?, ?> errorMap = (Map<?, ?>) error;
				detail = errorMap.get("code") + ": " + errorMap.get("message");
			}
		} catch (RuntimeException e) {
			classLogger.debug("Could not read the error Graph returned", e);
		}
		throw new IllegalArgumentException("Microsoft Graph would not " + what + ". Detailed error: " + detail);
	}

	/**
	 * Read the {@code value} collection out of a Graph response.
	 *
	 * @param response the response body
	 * @return the collection, empty when there is none
	 */
	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> readList(String response) {
		Map<String, Object> parsed = readMap(response);
		if (parsed == null) {
			return new ArrayList<>();
		}
		Object value = parsed.get("value");
		if (!(value instanceof List)) {
			return new ArrayList<>();
		}
		return (List<Map<String, Object>>) value;
	}

	/**
	 * @param response the response body
	 * @return the response as a map, or null when there is nothing to read
	 */
	private static Map<String, Object> readMap(String response) {
		if (response == null || response.trim().isEmpty()) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * Whether a message arrived before a cutoff, used to narrow a text search that
	 * could not carry a date filter.
	 *
	 * @param message the message
	 * @param since   the cutoff
	 * @return true when the message is older than the cutoff
	 */
	private static boolean receivedBefore(Map<String, Object> message, Date since) {
		Object received = message.get("receivedDateTime");
		if (received == null) {
			return false;
		}
		try {
			return Instant.parse(received.toString()).isBefore(since.toInstant());
		} catch (RuntimeException e) {
			classLogger.debug("Could not read the received date {}", received, e);
			return false;
		}
	}

	/**
	 * Add one set of recipients in the shape Graph reads them.
	 *
	 * @param message    the message being built
	 * @param key        the recipient collection to set
	 * @param recipients the addresses, or null when there are none
	 */
	private static void putRecipients(Map<String, Object> message, String key, String[] recipients) {
		if (recipients == null || recipients.length == 0) {
			return;
		}
		List<Map<String, Object>> addresses = new ArrayList<>();
		for (String recipient : recipients) {
			Map<String, Object> emailAddress = new LinkedHashMap<>();
			emailAddress.put("address", recipient);
			addresses.add(Map.of("emailAddress", emailAddress));
		}
		message.put(key, addresses);
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * What to look for in a mailbox.
	 *
	 * <p>
	 * The fields are plain because Graph treats them differently depending on which
	 * are set, and {@link MicrosoftOutlookMailHelper#listMessages} is where that
	 * knowledge belongs rather than in every caller.
	 */
	public static class MessageQuery {

		/** The folder to read, by well known name or id. Null reads them all. */
		public String folder = "inbox";

		/** How many messages to ask Graph for. */
		public int top = 10;

		/** Text the sender has to contain, which makes this a search. */
		public String from = null;

		/** Text the subject has to contain, which makes this a search. */
		public String subject = null;

		/** The oldest message to return. */
		public Date since = null;

		/** Whether to return only messages nobody has opened. */
		public boolean unreadOnly = false;

		/** Whether the body comes back with each message. */
		public boolean includeBody = true;

	}

}
