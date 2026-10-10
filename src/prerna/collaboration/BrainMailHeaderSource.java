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

import java.time.Instant;
import java.util.List;
import java.util.Map;

import prerna.auth.User;

// onboarding and Refresh: message headers only (no body), in Graph shape, for the import and the mailbox overview
public interface BrainMailHeaderSource {

	String INBOX = "inbox";
	String SENT = "sentitems";
	List<String> FOLDERS = List.of(INBOX, SENT);

	// the signed-in mailbox: id, displayName, mail, userPrincipalName
	Map<String, Object> me(User user) throws Exception;

	// the owner's manager (displayName, mail), or null
	Map<String, Object> manager(User user) throws Exception;

	// the owner's other addresses (aliases), lower case; empty when unknown
	default List<String> aliases(User user) {
		return List.of();
	}

	// the owner's organisation from the directory: name and verified domains; empty
	// when unknown
	default Map<String, Object> organization(User user) {
		return Map.of();
	}

	/**
	 * Directory entries by lower-case address: kind (person, guest, mailbox, list),
	 * name, title, department, company, id. Addresses the directory does not know
	 * are left out; complete is false when the lookup stopped early (no scope,
	 * throttled), so a miss there proves nothing.
	 */
	record Directory(Map<String, Map<String, Object>> entries, boolean complete) {
	}

	// null when there is no directory (fixtures, other mail sources)
	default Directory lookup(User user, List<String> addresses) {
		return null;
	}

	// people in the directory whose name or address matches the query: name, email, title, department; empty when
	// there is no directory
	default List<Map<String, Object>> searchDirectory(User user, String query, int max) {
		return List.of();
	}

	// the owner's direct reports and peers (the manager's other reports), address
	// to "report" or "peer"
	default Map<String, String> orgChart(User user, String managerId) {
		return Map.of();
	}

	// headers received at or after since, newest first, at most max
	List<Map<String, Object>> list(User user, String folder, Instant since, int max) throws Exception;

	// Topic discovery additionally needs unsubscribe headers; fixtures already retain those when supplied.
	default List<Map<String, Object>> topicHeaders(User user, String folder, Instant since, int max) throws Exception {
		return list(user, folder, since, max);
	}

	/**
	 * Teams chat messages sent at or after since, in the header shape: id (the
	 * message), internetMessageId (chat and message), conversationId (the chat),
	 * subject (the chat's), from, toRecipients (the other members),
	 * receivedDateTime. System and app messages and senders without an address are
	 * left out.
	 */
	default List<Map<String, Object>> chats(User user, Instant since, int maxChats, int maxPerChat) throws Exception {
		return List.of();
	}

	/**
	 * Readable chat headers and partial failures; a skipped chat keeps the sync checkpoint unchanged, an unreadable
	 * one (no access to it) does not.
	 */
	record ChatImport(List<Map<String, Object>> messages, int skippedChats, boolean reauthNeeded,
			int unreadableChats) {

		ChatImport(List<Map<String, Object>> messages, int skippedChats, boolean reauthNeeded) {
			this(messages, skippedChats, reauthNeeded, 0);
		}
	}

	// Existing sources that only supply headers have no partial failures to report.
	default ChatImport importChats(User user, Instant since, int maxChats, int maxPerChat) throws Exception {
		return new ChatImport(chats(user, since, maxChats, maxPerChat), 0, false);
	}

	/** Chats read so far out of every chat in the window, while a long Teams read runs. */
	@FunctionalInterface
	interface ChatProgress {
		void read(int done, int total);
	}

	default ChatImport importChats(User user, Instant since, int maxChats, int maxPerChat, ChatProgress progress)
			throws Exception {
		return importChats(user, since, maxChats, maxPerChat);
	}

	// HttpHelperUtility exposes the status in its exception message. A 403 describes access, not expired login.
	static boolean needsReauth(Throwable error) {
		for (Throwable cause = error; cause != null; cause = cause.getCause()) {
			if (String.valueOf(cause.getMessage()).matches("(?s).*returned HTTP 401\\b.*")) {
				return true;
			}
		}
		return false;
	}

	// a chat the owner no longer has access to (often a meeting chat) fails the same way every time
	static boolean noAccess(Throwable error) {
		for (Throwable cause = error; cause != null; cause = cause.getCause()) {
			if (String.valueOf(cause.getMessage()).matches("(?s).*returned HTTP 40[34]\\b.*")) {
				return true;
			}
		}
		return false;
	}

	static BrainMailHeaderSource current() {
		String fixture = BrainMessageSource.fixturePath();
		return fixture == null ? new BrainGraphHeaderSource() : BrainFixtureHeaderSource.of(fixture);
	}
}
