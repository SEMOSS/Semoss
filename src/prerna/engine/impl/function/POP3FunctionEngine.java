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
package prerna.engine.impl.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import prerna.engine.api.FunctionTypeEnum;

/**
 * Function engine that reads a mailbox over POP3.
 *
 * <p>
 * POP3 is the plainest thing a mail server offers: one inbox, no folders, no
 * record of what has been read, and no search. That makes it the right engine
 * for a mailbox that exists to be drained by something other than a person - a
 * reports address, an alerts address, an inbox a process watches - and the
 * wrong one for a real person's mail, which wants {@link IMAPFunctionEngine}.
 *
 * <p>
 * Because the server cannot search, this engine filters the mailbox itself,
 * walking back from the newest message and stopping once it has enough matches.
 * A search that matches nothing therefore costs a read of the whole mailbox, so
 * an engine pointed at a large one wants a narrow {@code sinceDays}.
 *
 * <p>
 * Nothing here removes a message. POP3 deletion happens on the server the
 * moment the connection closes and cannot be undone, and a mailbox that a
 * process is draining is exactly where that would be least recoverable.
 */
public class POP3FunctionEngine extends AbstractMailStoreFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(POP3FunctionEngine.class);

	// public so a caller building this engine in memory rather than from a
	// catalogued SMSS can populate the properties by name
	public static final String POP3_HOST_KEY = "POP3_" + HOST_SUFFIX;
	public static final String POP3_PORT_KEY = "POP3_" + PORT_SUFFIX;
	public static final String POP3_USERNAME_KEY = "POP3_" + USERNAME_SUFFIX;
	public static final String POP3_PASSWORD_KEY = "POP3_" + PASSWORD_SUFFIX;
	public static final String POP3_SECURITY_KEY = "POP3_" + SECURITY_SUFFIX;

	private static final String PROTOCOL = "pop3";
	private static final String SECURE_PROTOCOL = "pop3s";

	// 995 is the encrypted port, 110 the one that starts in the clear
	private static final String DEFAULT_SECURE_PORT = "995";
	private static final String DEFAULT_PORT = "110";

	/**
	 * Build a POP3 engine that is not in the catalog, for a caller that already
	 * holds a mailbox configuration and wants this engine's connection handling
	 * rather than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mailbox properties, either this engine's own keys or raw
	 *                 {@code mail.} keys
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mailbox
	 */
	public static POP3FunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		POP3FunctionEngine engine = new POP3FunctionEngine();
		// no folder structure, no secret store lookup, no catalog entry
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Read the email in the " + engineId + " mailbox"));
		return engine;
	}

	@Override
	protected Message[] findMessages(Folder folder, MailSearchCriteria criteria) throws MessagingException {
		if (criteria.unreadOnly) {
			classLogger.warn("POP3 does not record which messages have been read, so {} is ignored", UNREAD_ONLY_PARAM);
		}

		Message[] all = folder.getMessages();
		if (all == null || all.length == 0 || criteria.isEmpty()) {
			return all;
		}

		// walked newest first and stopped at the cap, so a search on a mailbox of
		// thousands does not read all of them to answer for the last few
		List<Message> matches = new ArrayList<>();
		for (int i = all.length - 1; i >= 0 && matches.size() < this.maxMessages; i--) {
			if (matches(all[i], criteria)) {
				matches.add(all[i]);
			}
		}
		// oldest first, which is the order a folder hands its messages back in and
		// what the search is read in
		Collections.reverse(matches);
		return matches.toArray(new Message[0]);
	}

	/**
	 * Whether one message matches the search, checked here rather than on the
	 * server because POP3 has no search command.
	 *
	 * @param message  the message to check
	 * @param criteria what the caller asked to match on
	 * @return true when the message matches everything that was asked
	 * @throws MessagingException when the message cannot be read
	 */
	private boolean matches(Message message, MailSearchCriteria criteria) throws MessagingException {
		if (criteria.since != null) {
			Date sent = message.getSentDate();
			if (sent == null) {
				// a POP3 server does not stamp an arrival time, so the date the sender
				// put on the message is all there is to go on
				sent = message.getReceivedDate();
			}
			if (sent == null || sent.before(criteria.since)) {
				return false;
			}
		}
		if (criteria.subject != null) {
			String subject = message.getSubject();
			if (subject == null || !subject.toLowerCase().contains(criteria.subject.toLowerCase())) {
				return false;
			}
		}
		if (criteria.from != null) {
			String from = joinAddresses(message.getFrom());
			if (from == null || !from.toLowerCase().contains(criteria.from.toLowerCase())) {
				return false;
			}
		}
		return true;
	}

	@Override
	protected String getDefaultFunctionDescription() {
		return """
				Read the email sitting in a POP3 mailbox. Use this to see what has arrived, find a message \
				about something, or pull what a message says so it can be acted on. The mailbox has a single \
				inbox and no record of what has already been read, so a search returns the most recent \
				messages that match whether or not anyone has seen them before.\
				""";
	}

	@Override
	protected String getProtocol() {
		return PROTOCOL;
	}

	@Override
	protected String getSecureProtocol() {
		return SECURE_PROTOCOL;
	}

	@Override
	protected String getDefaultPort(boolean secure) {
		return secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.POP3.name();
	}

}
