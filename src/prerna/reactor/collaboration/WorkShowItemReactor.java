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
package prerna.reactor.collaboration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import prerna.auth.User;
import prerna.collaboration.BrainThreadMessages;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.teams.MicrosoftTeamsMessageHelper;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkShowItem(emailRef=["email:..."]); or threadId, chatId, or path instead.
// Checks one item the assistant found and returns its exact ids; the owner's chat
// shows it beside the conversation. Nothing is matched by subject or file name.
public class WorkShowItemReactor extends AbstractCollaborationReactor {

	private static final String EMAIL_REF = "emailRef";
	private static final String THREAD_ID = "threadId";
	private static final String CHAT_ID = "chatId";
	private static final String PATH = "path";
	private static final String NOT_SHOWN = " Nothing was shown.";
	// enough messages to tell a readable thread from one its rules or deletions emptied
	private static final int THREAD_CHECK_LIMIT = 20;
	// Graph chat ids go into the request path unencoded, so only their own characters pass
	private static final Pattern CHAT_ID_PATTERN = Pattern.compile("[A-Za-z0-9:@._=-]{1,256}");

	public WorkShowItemReactor() {
		this.keysToGet = new String[] { EMAIL_REF, THREAD_ID, CHAT_ID, PATH };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String emailRef = value(EMAIL_REF);
		String threadId = value(THREAD_ID);
		String chatId = value(CHAT_ID);
		String path = value(PATH);
		int items = (emailRef != null || threadId != null ? 1 : 0) + (chatId != null ? 1 : 0) + (path != null ? 1 : 0);
		if (items != 1) {
			throw new IllegalArgumentException(
					"Pass one item: emailRef (optionally with its threadId), threadId, chatId, or path." + NOT_SHOWN);
		}
		Map<String, Object> item;
		if (emailRef != null) {
			item = email(user, emailRef, threadId);
		} else if (threadId != null) {
			item = thread(user, threadId);
		} else if (chatId != null) {
			item = chat(user, chatId);
		} else {
			item = file(path);
		}
		item.put("note", "Shown to the owner beside the chat. Nothing was changed.");
		return mapResult(item);
	}

	private static Map<String, Object> email(User user, String emailRef, String threadId) {
		Map<String, Object> email = BrainThreadMessages.composeSource(user, emailRef);
		if (email == null || !(email.get("threadId") instanceof String)) {
			throw new IllegalArgumentException("That email is not available: it is not in the owner's Brain, was "
					+ "deleted or moved, or their rules hide it now. Pass emailRef exactly as ReadThread returned it."
					+ NOT_SHOWN);
		}
		Object emailThread = email.get("threadId");
		if (threadId != null && !threadId.equals(emailThread)) {
			throw new IllegalArgumentException("That email is not on thread " + threadId
					+ ". Pass the emailRef and threadId from the same ReadThread result." + NOT_SHOWN);
		}
		Map<String, Object> item = item("thread");
		item.put("threadId", emailThread);
		item.put("source", "email");
		item.put("messageId", email.get("id"));
		item.put("title", email.get("subject"));
		return item;
	}

	private static Map<String, Object> thread(User user, String threadId) {
		Map<String, Object> read;
		try {
			read = BrainThreadMessages.read(user, threadId, THREAD_CHECK_LIMIT);
		} catch (IllegalArgumentException e) {
			if ("Thread not found".equals(e.getMessage())) {
				throw new IllegalArgumentException("No thread " + threadId
						+ " is in the owner's Brain. Copy threadId exactly from SearchMail or ReadThread." + NOT_SHOWN, e);
			}
			throw e;
		}
		Object source = read.get("source");
		if (!"email".equals(source) && !"teams".equals(source)) {
			throw new IllegalArgumentException(
					"Only email threads and Teams chats can be shown, not a " + source + " thread." + NOT_SHOWN);
		}
		// messages come oldest first; the newest subject names the thread
		String title = "";
		boolean readable = false;
		List<?> messages = read.get("messages") instanceof List<?> list ? list : List.of();
		for (Object message : messages) {
			if (message instanceof Map<?, ?> entry && !Boolean.TRUE.equals(entry.get("excluded"))) {
				readable = true;
				if (entry.get("subject") instanceof String subject && !subject.isBlank()) {
					title = subject;
				}
			}
		}
		if (!readable) {
			throw new IllegalArgumentException("That thread has nothing the owner can read here: its messages were "
					+ "deleted or moved, or their rules hide them." + NOT_SHOWN);
		}
		Map<String, Object> item = item("thread");
		item.put("threadId", threadId);
		item.put("source", source);
		item.put("title", title);
		return item;
	}

	private static Map<String, Object> chat(User user, String chatId) {
		if (!CHAT_ID_PATTERN.matcher(chatId).matches()) {
			throw new IllegalArgumentException(
					"That is not a Teams chat id. Copy chatId exactly from ListChats or ListChatMessages." + NOT_SHOWN);
		}
		String token;
		try {
			token = MicrosoftLoginUtils.getValidAccessToken(user);
		} catch (RuntimeException e) {
			// a missing Microsoft login keeps its own error, which asks the owner to sign in
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Could not sign in to Microsoft: " + e.getMessage() + NOT_SHOWN, e);
		}
		Map<String, Object> chat;
		try {
			chat = MicrosoftTeamsMessageHelper.getChat(token, chatId, MicrosoftLoginUtils.getMicrosoftEmail(user));
			// the owner's chat reads the messages, so a chat they can list but not read fails now instead
			MicrosoftTeamsMessageHelper.listChatMessages(token, chatId, 1, 1);
		} catch (Exception e) {
			String error = String.valueOf(e.getMessage());
			if (error.contains("HTTP 404") || error.contains("HTTP 403") || error.contains("NotFound")
					|| error.contains("Forbidden")) {
				throw new IllegalArgumentException(
						"That Teams chat was not found, or the owner cannot read it." + NOT_SHOWN, e);
			}
			throw new IllegalStateException("That Teams chat could not be read: " + error + NOT_SHOWN, e);
		}
		Map<String, Object> item = item("chat");
		item.put("chatId", chat.get("id") instanceof String id && !id.isBlank() ? id : chatId);
		item.put("title", chat.get("displayName") instanceof String name ? name : "");
		if (chat.get("webUrl") instanceof String link && link.startsWith("https://")) {
			item.put("webLink", link);
		}
		return item;
	}

	private Map<String, Object> file(String path) {
		String relative = path.replaceFirst("^/+", "");
		String folder = this.insight.getInsightFolder();
		Path file;
		Path root;
		try {
			file = MCPUtility.resolveContainedMcpFile(folder, relative);
			root = Path.of(folder).toAbsolutePath().normalize().toRealPath();
		} catch (IOException | IllegalArgumentException e) {
			throw new IllegalArgumentException("No file at " + path
					+ " in this chat. Download it first, then pass the path its result gives." + NOT_SHOWN, e);
		}
		Map<String, Object> item = item("file");
		item.put("path", "/" + root.relativize(file).toString().replace('\\', '/'));
		item.put("name", file.getFileName().toString());
		return item;
	}

	private static Map<String, Object> item(String kind) {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("shown", true);
		item.put("kind", kind);
		return item;
	}

	private String value(String key) {
		String value = getString(key);
		return value == null || value.isBlank() ? null : value.trim();
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.UI_LOADING_MESSAGE, "Opening...");
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Shows the owner one item you found, beside the chat: an email, an email thread, a Teams chat, or a "
				+ "file in this chat. Use it only when the owner asks to see, open, or bring up an item. Pass exactly one "
				+ "item, with its id copied from an earlier result in this chat. It checks that the item still exists and "
				+ "that the owner can read it, and fails otherwise. It changes nothing.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (EMAIL_REF.equals(key)) {
			return "One email: its emailRef (email:...) from ReadThread";
		}
		if (THREAD_ID.equals(key)) {
			return "A whole email thread or Teams chat: its threadId from SearchMail or ReadThread. With emailRef, "
					+ "the thread that email is on";
		}
		if (CHAT_ID.equals(key)) {
			return "A Teams chat that Brain does not hold: its chat id from ListChats or ListChatMessages";
		}
		if (PATH.equals(key)) {
			return "A file in this chat's working directory: the path a download returned, for example report.pdf";
		}
		return super.getDescriptionForKey(key);
	}
}
