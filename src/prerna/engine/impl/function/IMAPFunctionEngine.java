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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.UIDFolder;
import jakarta.mail.search.AndTerm;
import jakarta.mail.search.ComparisonTerm;
import jakarta.mail.search.FlagTerm;
import jakarta.mail.search.FromStringTerm;
import jakarta.mail.search.ReceivedDateTerm;
import jakarta.mail.search.SearchTerm;
import jakarta.mail.search.SubjectTerm;
import prerna.engine.api.FunctionTypeEnum;

/**
 * Function engine that reads a mailbox over IMAP.
 *
 * <p>
 * Unlike POP3, the mailbox stays on the server: there are folders, the server
 * remembers which messages have been read, it does the searching, and every
 * message has a uid that is still valid on the next connection. That uid is
 * what makes it possible to do more than read - a caller can hand one back to
 * mark a message, file it, or throw it away.
 *
 * <p>
 * Those changes are the reason this engine is more locked down than it looks. A
 * search opens the folder read only, so nothing is marked seen just because
 * something looked at it, and every way of changing the mailbox is off until an
 * admin turns it on in the SMSS. Deleting is the one that cannot be walked
 * back: IMAP has no move either, so filing a message is a copy followed by a
 * delete of the original, and both only happen with {@link #ALLOW_MOVE_KEY}
 * set.
 */
public class IMAPFunctionEngine extends AbstractMailStoreFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(IMAPFunctionEngine.class);

	// public so a caller building this engine in memory rather than from a
	// catalogued SMSS can populate the properties by name
	public static final String IMAP_HOST_KEY = "IMAP_" + HOST_SUFFIX;
	public static final String IMAP_PORT_KEY = "IMAP_" + PORT_SUFFIX;
	public static final String IMAP_USERNAME_KEY = "IMAP_" + USERNAME_SUFFIX;
	public static final String IMAP_PASSWORD_KEY = "IMAP_" + PASSWORD_SUFFIX;
	public static final String IMAP_SECURITY_KEY = "IMAP_" + SECURITY_SUFFIX;

	public static final String DEFAULT_FOLDER_KEY = "DEFAULT_FOLDER";
	public static final String ALLOWED_FOLDERS_KEY = "ALLOWED_FOLDERS";
	public static final String MARK_AS_READ_KEY = "MARK_AS_READ";
	public static final String ALLOW_FLAG_CHANGES_KEY = "ALLOW_FLAG_CHANGES";
	public static final String ALLOW_MOVE_KEY = "ALLOW_MOVE";
	public static final String ALLOW_DELETE_KEY = "ALLOW_DELETE";

	// the actions execute understands beyond searching
	public static final String MARK_READ_ACTION = "markRead";
	public static final String MARK_UNREAD_ACTION = "markUnread";
	public static final String MOVE_ACTION = "move";
	public static final String DELETE_ACTION = "delete";

	private static final String UID_PARAM = "uid";
	private static final String TARGET_FOLDER_PARAM = "targetFolder";

	private static final String PROTOCOL = "imap";
	private static final String SECURE_PROTOCOL = "imaps";

	// 993 is the encrypted port, 143 the one that starts in the clear
	private static final String DEFAULT_SECURE_PORT = "993";
	private static final String DEFAULT_PORT = "143";

	private String defaultFolder = INBOX_FOLDER;

	// blank means any folder of the mailbox can be named
	private Set<String> allowedFolders = new LinkedHashSet<>();

	private boolean markAsRead = false;
	private boolean allowFlagChanges = false;
	private boolean allowMove = false;
	private boolean allowDelete = false;

	/**
	 * Build an IMAP engine that is not in the catalog, for a caller that already
	 * holds a mailbox configuration and wants this engine's connection handling
	 * rather than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mailbox properties, either this engine's own keys or raw
	 *                 {@code mail.} keys
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mailbox
	 */
	public static IMAPFunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		IMAPFunctionEngine engine = new IMAPFunctionEngine();
		// no folder structure, no secret store lookup, no catalog entry
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Read the email in the " + engineId + " mailbox"));
		return engine;
	}

	@Override
	protected void openProtocolProperties(Properties smssProp) {
		this.defaultFolder = StringUtils.defaultIfEmpty(trimToNull(smssProp.getProperty(DEFAULT_FOLDER_KEY)),
				this.defaultFolder);
		this.allowedFolders.addAll(splitList(smssProp.getProperty(ALLOWED_FOLDERS_KEY)));
		this.markAsRead = parseBoolean(smssProp.getProperty(MARK_AS_READ_KEY), this.markAsRead);
		this.allowFlagChanges = parseBoolean(smssProp.getProperty(ALLOW_FLAG_CHANGES_KEY), this.allowFlagChanges);
		this.allowMove = parseBoolean(smssProp.getProperty(ALLOW_MOVE_KEY), this.allowMove);
		this.allowDelete = parseBoolean(smssProp.getProperty(ALLOW_DELETE_KEY), this.allowDelete);

		if (!isAllowedFolder(this.defaultFolder)) {
			throw new IllegalArgumentException("The " + DEFAULT_FOLDER_KEY + " of " + this.defaultFolder
					+ " is not one of the " + ALLOWED_FOLDERS_KEY + " = " + this.allowedFolders);
		}
		if (this.markAsRead) {
			// worth a line in the log, since it means reading the mailbox changes it
			classLogger.info("{} is enabled, so a message this engine returns is marked as read", MARK_AS_READ_KEY);
		}
	}

	@Override
	protected Message[] findMessages(Folder folder, MailSearchCriteria criteria) throws MessagingException {
		if (criteria.isEmpty()) {
			return folder.getMessages();
		}

		List<SearchTerm> terms = new ArrayList<>();
		if (criteria.unreadOnly) {
			terms.add(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
		}
		if (criteria.from != null) {
			terms.add(new FromStringTerm(criteria.from));
		}
		if (criteria.subject != null) {
			terms.add(new SubjectTerm(criteria.subject));
		}
		if (criteria.since != null) {
			terms.add(new ReceivedDateTerm(ComparisonTerm.GE, criteria.since));
		}

		if (terms.size() == 1) {
			return folder.search(terms.get(0));
		}
		return folder.search(new AndTerm(terms.toArray(new SearchTerm[0])));
	}

	@Override
	protected Object executeMailAction(String action, Map<String, Object> parameterValues) {
		String folderName = resolveFolderName(getParameterValue(parameterValues, FOLDER_PARAM, null));
		List<Long> uids = parseUids(parameterAsList(parameterValues, UID_PARAM));

		if (MARK_READ_ACTION.equalsIgnoreCase(action)) {
			return actionOutput(MARK_READ_ACTION, folderName, uids, markMessages(folderName, uids, true));
		}
		if (MARK_UNREAD_ACTION.equalsIgnoreCase(action)) {
			return actionOutput(MARK_UNREAD_ACTION, folderName, uids, markMessages(folderName, uids, false));
		}
		if (MOVE_ACTION.equalsIgnoreCase(action)) {
			String targetFolder = getParameterValue(parameterValues, TARGET_FOLDER_PARAM, null);
			if (targetFolder == null) {
				throw new IllegalArgumentException(
						"Must define the " + TARGET_FOLDER_PARAM + " parameter to know where to move the message to");
			}
			Map<String, Object> output = actionOutput(MOVE_ACTION, folderName, uids,
					moveMessages(folderName, uids, targetFolder));
			output.put(TARGET_FOLDER_PARAM, targetFolder);
			return output;
		}
		if (DELETE_ACTION.equalsIgnoreCase(action)) {
			return actionOutput(DELETE_ACTION, folderName, uids, deleteMessages(folderName, uids));
		}
		throw new IllegalArgumentException(
				"The '" + action + "' action is not something this function engine can do. The actions available are "
						+ availableActions());
	}

	/**
	 * Mark messages as read or unread.
	 *
	 * @param folderName the folder the messages are in
	 * @param uids       the uids of the messages, as returned by a search
	 * @param seen       true to mark read, false to mark unread
	 * @return how many messages were changed
	 */
	public int markMessages(String folderName, List<Long> uids, boolean seen) {
		if (!this.allowFlagChanges) {
			throw new IllegalArgumentException("This function engine cannot change whether a message is read. Set "
					+ ALLOW_FLAG_CHANGES_KEY + " in the SMSS to allow it");
		}
		return applyToMessages(folderName, uids, false,
				(folder, messages) -> folder.setFlags(messages, new Flags(Flags.Flag.SEEN), seen));
	}

	/**
	 * File messages into another folder of the same mailbox.
	 *
	 * <p>
	 * IMAP has no move, so this is a copy into the target folder followed by
	 * deleting the originals. A server that refuses the copy therefore leaves the
	 * messages where they were rather than losing them.
	 *
	 * @param folderName   the folder the messages are in
	 * @param uids         the uids of the messages, as returned by a search
	 * @param targetFolder the folder to move them to
	 * @return how many messages were moved
	 */
	public int moveMessages(String folderName, List<Long> uids, String targetFolder) {
		if (!this.allowMove) {
			throw new IllegalArgumentException(
					"This function engine cannot move a message. Set " + ALLOW_MOVE_KEY + " in the SMSS to allow it");
		}
		if (!isAllowedFolder(targetFolder)) {
			throw new IllegalArgumentException("This function engine can only use the folders " + this.allowedFolders
					+ " but " + targetFolder + " was requested");
		}
		if (targetFolder.equalsIgnoreCase(folderName)) {
			throw new IllegalArgumentException("The messages are already in " + folderName);
		}
		return applyToMessages(folderName, uids, true, (folder, messages) -> {
			Folder destination = folder.getStore().getFolder(targetFolder);
			if (destination == null || !destination.exists()) {
				throw new IllegalArgumentException("The mailbox does not have a folder named " + targetFolder);
			}
			folder.copyMessages(messages, destination);
			// the copy is what makes this a move. the originals are removed when the
			// folder closes, so a failed copy takes nothing with it
			folder.setFlags(messages, new Flags(Flags.Flag.DELETED), true);
		});
	}

	/**
	 * Delete messages from the mailbox. There is no undo on the server.
	 *
	 * @param folderName the folder the messages are in
	 * @param uids       the uids of the messages, as returned by a search
	 * @return how many messages were deleted
	 */
	public int deleteMessages(String folderName, List<Long> uids) {
		if (!this.allowDelete) {
			throw new IllegalArgumentException("This function engine cannot delete a message. Set " + ALLOW_DELETE_KEY
					+ " in the SMSS to allow it");
		}
		return applyToMessages(folderName, uids, true,
				(folder, messages) -> folder.setFlags(messages, new Flags(Flags.Flag.DELETED), true));
	}

	/**
	 * Open a folder for writing, find the messages a caller named, and change them.
	 *
	 * @param folderName the folder the messages are in
	 * @param uids       the uids of the messages, as returned by a search
	 * @param expunge    whether messages flagged as deleted are removed when the
	 *                   folder closes
	 * @param action     what to do to the messages
	 * @return how many messages were changed
	 */
	private int applyToMessages(String folderName, List<Long> uids, boolean expunge, FolderAction action) {
		if (uids == null || uids.isEmpty()) {
			throw new IllegalArgumentException(
					"Must define the " + UID_PARAM + " parameter to know which message to act on");
		}
		if (uids.size() > this.maxMessages) {
			throw new IllegalArgumentException("This function engine acts on at most " + this.maxMessages
					+ " messages per call but " + uids.size() + " were provided");
		}

		Folder folder = null;
		try {
			folder = openFolder(folderName, Folder.READ_WRITE);
			Message[] messages = messagesByUid(folder, uids);
			if (messages.length == 0) {
				return 0;
			}
			action.apply(folder, messages);
			return messages.length;
		} catch (MessagingException e) {
			classLogger.error("Error updating the messages in the " + folderName + " folder of " + this.host, e);
			throw new IllegalArgumentException("Error occurred updating the mailbox. Detailed error: " + e.getMessage(),
					e);
		} finally {
			closeFolder(folder, expunge);
		}
	}

	/**
	 * Look up the messages a caller named by uid. A uid that is no longer in the
	 * folder is skipped rather than failing the call, since a message someone else
	 * already moved is not an error worth losing the rest of the call over.
	 *
	 * @param folder the open folder
	 * @param uids   the uids to look up
	 * @return the messages that are still there
	 * @throws MessagingException when the folder cannot be read
	 */
	private Message[] messagesByUid(Folder folder, List<Long> uids) throws MessagingException {
		if (!(folder instanceof UIDFolder)) {
			throw new IllegalArgumentException(
					"The mail server does not identify messages by uid, so they cannot be acted on one at a time");
		}
		long[] rawUids = new long[uids.size()];
		for (int i = 0; i < rawUids.length; i++) {
			rawUids[i] = uids.get(i).longValue();
		}

		List<Message> messages = new ArrayList<>();
		Message[] found = ((UIDFolder) folder).getMessagesByUID(rawUids);
		if (found != null) {
			for (Message message : found) {
				if (message != null) {
					messages.add(message);
				}
			}
		}
		if (messages.size() < rawUids.length) {
			classLogger.warn("{} of the {} uids passed in are no longer in the folder",
					rawUids.length - messages.size(), rawUids.length);
		}
		return messages.toArray(new Message[0]);
	}

	/**
	 * Describe what one change did, so the caller can repeat it back rather than
	 * guess at it.
	 *
	 * @param action     the action that ran
	 * @param folderName the folder it ran against
	 * @param uids       the uids that were asked for
	 * @param affected   how many messages were actually changed
	 * @return the output map
	 */
	private Map<String, Object> actionOutput(String action, String folderName, List<Long> uids, int affected) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("action", action);
		output.put("folder", folderName);
		output.put("requested", uids.size());
		output.put("affected", affected);
		output.put("uids", uids);
		return output;
	}

	/**
	 * Read the uids a caller passed in.
	 *
	 * @param values the raw values
	 * @return the uids
	 */
	private static List<Long> parseUids(List<String> values) {
		List<Long> uids = new ArrayList<>();
		for (String value : values) {
			try {
				uids.add(Long.valueOf(value));
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(
						"The value '" + value + "' is not a message uid returned by a search", e);
			}
		}
		return uids;
	}

	/**
	 * Which of the changes to the mailbox this engine was allowed to make.
	 *
	 * @return the action names, empty when the engine only reads
	 */
	private List<String> availableActions() {
		List<String> actions = new ArrayList<>();
		if (this.allowFlagChanges) {
			actions.add(MARK_READ_ACTION);
			actions.add(MARK_UNREAD_ACTION);
		}
		if (this.allowMove) {
			actions.add(MOVE_ACTION);
		}
		if (this.allowDelete) {
			actions.add(DELETE_ACTION);
		}
		return actions;
	}

	/**
	 * Whether a folder is one this engine will touch.
	 *
	 * @param folderName the folder a caller named
	 * @return true when the folder can be used
	 */
	private boolean isAllowedFolder(String folderName) {
		if (this.allowedFolders.isEmpty()) {
			return true;
		}
		for (String allowedFolder : this.allowedFolders) {
			if (allowedFolder.equalsIgnoreCase(folderName)) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected String resolveFolderName(String requestedFolder) {
		if (requestedFolder == null) {
			return this.defaultFolder;
		}
		if (!isAllowedFolder(requestedFolder)) {
			throw new IllegalArgumentException("This function engine can only use the folders " + this.allowedFolders
					+ " but " + requestedFolder + " was requested");
		}
		return requestedFolder;
	}

	@Override
	protected int getFolderOpenMode() {
		// read only unless the engine was told to mark what it reads, so looking at a
		// mailbox does not quietly change what its owner sees as new
		return this.markAsRead ? Folder.READ_WRITE : Folder.READ_ONLY;
	}

	@Override
	protected void afterMessageRead(Folder folder, Message message) throws MessagingException {
		if (this.markAsRead) {
			message.setFlag(Flags.Flag.SEEN, true);
		}
	}

	@Override
	protected Long getMessageUid(Folder folder, Message message) throws MessagingException {
		if (folder instanceof UIDFolder) {
			return Long.valueOf(((UIDFolder) folder).getUID(message));
		}
		return null;
	}

	@Override
	protected boolean supportsFlags() {
		return true;
	}

	@Override
	protected void addProtocolParameters(List<FunctionParameter> parameters) {
		List<String> actions = availableActions();
		if (!actions.isEmpty()) {
			parameters.add(new FunctionParameter(ACTION_PARAM, "string", """
					Optional. What to do with the mailbox. Defaults to %s, which finds messages and returns \
					them. The other actions available are %s, and each one needs the uid of a message that a \
					search returned. An action other than %s changes the mailbox for everyone who reads it, \
					so confirm the messages before calling it.\
					""".formatted(SEARCH_ACTION, String.join(", ", actions), SEARCH_ACTION)));
			parameters.add(new FunctionParameter(UID_PARAM, "string", """
					The uid of the message to act on, or a comma separated list of them. Only used by an \
					action other than %s.\
					""".formatted(SEARCH_ACTION)));
			if (this.allowMove) {
				parameters.add(new FunctionParameter(TARGET_FOLDER_PARAM, "string", """
						The folder to move the message to. Only used by the %s action.\
						""".formatted(MOVE_ACTION) + allowedFolderText()));
			}
		}
		parameters.add(new FunctionParameter(FOLDER_PARAM, "string", """
				Optional. The folder of the mailbox to use.\
				""" + defaultText(this.defaultFolder) + allowedFolderText()));
		parameters.add(new FunctionParameter(UNREAD_ONLY_PARAM, "boolean", """
				Optional. Set to true to only return messages that have not been read yet. Defaults to false.\
				"""));
	}

	/**
	 * Build the sentence that tells a caller which folders it can name, so a model
	 * does not waste a call on one that will be refused.
	 *
	 * @return the sentence to append, or an empty string when there is no limit
	 */
	private String allowedFolderText() {
		if (this.allowedFolders.isEmpty()) {
			return "";
		}
		return " Only these folders can be used: " + String.join(", ", this.allowedFolders) + ".";
	}

	@Override
	protected String getDefaultFunctionDescription() {
		StringBuilder description = new StringBuilder("""
				Read the email in an IMAP mailbox. Use this to see what has arrived, find a message about \
				something, or pull what a message says so it can be acted on. Reading a message does not \
				mark it as read.\
				""");
		List<String> actions = availableActions();
		if (!actions.isEmpty()) {
			description.append(" This mailbox can also be changed through the ").append(ACTION_PARAM)
					.append(" parameter: ").append(String.join(", ", actions))
					.append(". Those take effect for everyone who reads the mailbox and cannot be undone.");
		}
		return description.toString();
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
		return FunctionTypeEnum.IMAP.name();
	}

	/**
	 * One change to a set of messages, so the folder handling around it is written
	 * once rather than per action.
	 */
	private interface FolderAction {

		/**
		 * @param folder   the open folder
		 * @param messages the messages to change
		 * @throws MessagingException when the mail server refuses the change
		 */
		void apply(Folder folder, Message[] messages) throws MessagingException;

	}

}
