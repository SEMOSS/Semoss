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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import prerna.auth.User;
import prerna.io.connector.ConnectorPage;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.io.connector.mail.AbstractListMailReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailMessage;
import prerna.query.parsers.MailSearchParser;

/**
 * Reads the mail of the signed in user's own Gmail mailbox.
 *
 * <p>
 * Required Google scope, each under {@code https://www.googleapis.com/auth/}:
 * one of {@code gmail.readonly} or {@code gmail.modify}.
 * </p>
 */
public class GoogleGmailListMailReactor extends AbstractListMailReactor {

	/**
	 * What Gmail's search syntax reads as its own, taken as plain text in a search.
	 */
	private static final Pattern SEARCH_SYNTAX = Pattern.compile("[\"(){}:]");

	/**
	 * A word naming one of Gmail's own search operators, such as has:attachment.
	 */
	private static final Pattern SEARCH_OPERATOR = Pattern.compile("[A-Za-z_]+:[A-Za-z0-9@._+\\-]+");

	/** The words Gmail joins terms with, searched as words instead. */
	private static final Set<String> SEARCH_JOINS = Set.of("OR", "AND", "AROUND");

	@Override
	protected MailApp getMailApp() {
		return MailApp.GMAIL;
	}

	@Override
	protected ConnectorPage<MailMessage> listMail(User user, ListMailRequest request) throws Exception {
		GoogleGmailHelper gmail = new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user));
		String account = accountEmail(user);

		if (request.conversationId() != null) {
			// a thread holds the replies the user sent as well as the ones they
			// received, oldest first, and is read newest first like every listing
			Map<String, Object> thread = gmail.getThread(request.conversationId());
			List<MailMessage> messages = new ArrayList<>();
			if (thread != null && thread.get("messages") instanceof List<?> found) {
				for (Object message : found) {
					if (message instanceof Map<?, ?> map) {
						messages.add(0,
								GoogleGmailMessageMapper.toMailMessage(cast(map), account, false, false, false, true));
					}
				}
			}
			return ConnectorPage.of(messages, request.offset(), request.limit());
		}

		GoogleGmailFolders.Target target = GoogleGmailFolders.resolve(gmail, request.folder());
		GoogleGmailHelper.IdPage page = gmail.listMessageIds(query(request, target), target.labelId(),
				request.needed());
		List<String> ids = page.ids();
		int from = Math.min(request.offset(), ids.size());
		int to = Math.min(request.offset() + request.limit(), ids.size());
		// only the page's own messages are read in full, one call each
		List<MailMessage> messages = new ArrayList<>();
		for (String id : ids.subList(from, to)) {
			messages.add(
					GoogleGmailMessageMapper.toMailMessage(gmail.getMessage(id), account, false, false, false, false));
		}
		return new ConnectorPage<>(messages, ids.size() > request.offset() + request.limit() || page.hasMore());
	}

	/**
	 * Write the filters as a Gmail search.
	 */
	private static String query(ListMailRequest request, GoogleGmailFolders.Target target) {
		List<String> terms = new ArrayList<>();
		if (target.archive()) {
			terms.add(GoogleGmailFolders.ARCHIVE_QUERY);
		}
		terms.addAll(searchTerms(request.terms()));
		if (request.unreadOnly()) {
			terms.add("is:unread");
		}
		if (request.since() != null) {
			terms.add("after:" + request.since().getEpochSecond());
		}
		return terms.isEmpty() ? null : String.join(" ", terms);
	}

	/**
	 * Write a search's terms the way Gmail's search box reads them, every term
	 * having to match.
	 *
	 * <p>
	 * Each word of a term is a term of its own, so words typed for the subject each
	 * have to be in it, and a phrase keeps its words together in quotes. Gmail has
	 * no operator for the body, so a body term searches the whole email, as a bare
	 * word does. What Gmail would read as its own, such as parentheses, a leading
	 * minus, and a bare {@code OR}, is searched as plain text, and a word such as
	 * {@code has:attachment} passes through as one of Gmail's own operators.
	 * </p>
	 *
	 * @param terms the search's terms
	 * @return the terms as Gmail reads them
	 */
	static List<String> searchTerms(List<MailSearchParser.Term> terms) {
		List<String> written = new ArrayList<>();
		for (MailSearchParser.Term term : terms) {
			MailSearchParser.Field field = term.field();
			String prefix = field == MailSearchParser.Field.ANY || field == MailSearchParser.Field.BODY ? ""
					: field.getPrefix() + ":";
			if (term.phrase()) {
				String phrase = SEARCH_SYNTAX.matcher(term.value()).replaceAll(" ").trim();
				if (!phrase.isEmpty()) {
					written.add(prefix + "\"" + phrase + "\"");
				}
				continue;
			}
			if (prefix.isEmpty() && SEARCH_OPERATOR.matcher(term.value()).matches()) {
				written.add(term.value());
				continue;
			}
			for (String word : SEARCH_SYNTAX.matcher(term.value()).replaceAll(" ").trim().split("\\s+")) {
				String plain = word.replaceFirst("^-+", "");
				if (!plain.isEmpty()) {
					written.add(prefix + (SEARCH_JOINS.contains(plain) ? plain.toLowerCase(Locale.ROOT) : plain));
				}
			}
		}
		return written;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> cast(Map<?, ?> map) {
		return (Map<String, Object>) map;
	}
}
