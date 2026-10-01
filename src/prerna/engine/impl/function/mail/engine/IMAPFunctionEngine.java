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
package prerna.engine.impl.function.mail.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.function.mail.adapter.jakarta.JakartaImapMailboxClient;
import prerna.engine.impl.function.mail.adapter.jakarta.auth.MailStoreAuthentication;
import prerna.engine.impl.function.mail.attachment.AttachmentStore;
import prerna.engine.impl.function.mail.config.JakartaStoreConfig;
import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.model.MailboxActionResult;
import prerna.engine.impl.function.mail.policy.MailReadPolicy;
import prerna.engine.impl.function.mail.spi.MailboxClient;

/**
 * Function engine that reads a mailbox over IMAP, and can change it when the
 * SMSS says so.
 *
 * <p>
 * Everything that alters a mailbox is off until it is turned on, one setting
 * per kind of change, and an engine cataloged without them is read only with no
 * way for a caller to talk it into anything else. That is the point of the
 * split: a mailbox somebody wants triaged and a mailbox somebody wants read are
 * the same engine with different permissions, and the second cannot become the
 * first by accident.
 *
 * <p>
 * An action that is not enabled is not merely refused, it is not published in
 * the function definition at all, so a model reading this engine's parameters
 * is never told about something it will then be denied.
 */
public class IMAPFunctionEngine extends AbstractMailStoreFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(IMAPFunctionEngine.class);

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

	public static final String MARK_READ_ACTION = "markRead";
	public static final String MARK_UNREAD_ACTION = "markUnread";
	public static final String MOVE_ACTION = "move";
	public static final String DELETE_ACTION = "delete";

	private static final String UID_PARAM = "uid";
	private static final String TARGET_FOLDER_PARAM = "targetFolder";
	private static final String PROTOCOL = "imap";
	private static final String SECURE_PROTOCOL = "imaps";
	private static final String DEFAULT_SECURE_PORT = "993";
	private static final String DEFAULT_PORT = "143";

	private String defaultFolder = INBOX_FOLDER;
	protected Set<String> allowedFolders = new LinkedHashSet<>();
	protected boolean markAsRead;
	protected boolean allowFlagChanges;
	protected boolean allowMove;
	protected boolean allowDelete;

	/**
	 * Build an IMAP engine that is not in the catalog, for a caller that already
	 * holds mailbox settings and wants this engine's connection handling rather
	 * than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mailbox properties
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mailbox
	 */
	public static IMAPFunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		IMAPFunctionEngine engine = new IMAPFunctionEngine();
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Read the email in the " + engineId + " mailbox"));
		return engine;
	}

	/**
	 * Read which folders this engine may touch and what it may do to them.
	 *
	 * <p>
	 * A default folder outside the allowlist is refused here rather than on the
	 * first search, since an engine configured that way could never answer anything
	 * and it is better to find out while cataloging it.
	 */
	@Override
	protected void openProtocolProperties(Properties properties) {
		this.defaultFolder = MailProperties
				.firstNonNull(MailProperties.trimToNull(properties.getProperty(DEFAULT_FOLDER_KEY)), INBOX_FOLDER);
		this.allowedFolders = new LinkedHashSet<>(
				MailProperties.splitList(properties.getProperty(ALLOWED_FOLDERS_KEY)));
		this.markAsRead = MailProperties.parseBoolean(properties.getProperty(MARK_AS_READ_KEY), false);
		this.allowFlagChanges = MailProperties.parseBoolean(properties.getProperty(ALLOW_FLAG_CHANGES_KEY), false);
		this.allowMove = MailProperties.parseBoolean(properties.getProperty(ALLOW_MOVE_KEY), false);
		this.allowDelete = MailProperties.parseBoolean(properties.getProperty(ALLOW_DELETE_KEY), false);
		if (!isAllowedFolder(this.defaultFolder)) {
			throw new IllegalArgumentException("The " + DEFAULT_FOLDER_KEY + " of " + this.defaultFolder
					+ " is not one of the " + ALLOWED_FOLDERS_KEY + " = " + this.allowedFolders);
		}
		if (this.markAsRead) {
			classLogger.info("{} is enabled, so returned messages are marked as read", MARK_AS_READ_KEY);
		}
	}

	@Override
	protected MailboxClient createJakartaClient(JakartaStoreConfig config, MailStoreAuthentication authentication,
			MailReadPolicy policy, AttachmentStore attachmentStore) {
		return new JakartaImapMailboxClient(config, authentication, policy, attachmentStore, this.markAsRead);
	}

	@Override
	protected boolean markGraphSearchAsRead() {
		return this.markAsRead;
	}

	/**
	 * Change the mailbox.
	 *
	 * <p>
	 * Every branch checks its own permission before touching anything, so the
	 * refusal names the SMSS key that would allow it rather than leaving whoever
	 * hits it to guess.
	 *
	 * @param action     the change to make
	 * @param parameters the runtime parameters for this call
	 * @return what the action did, including the uids that are valid afterwards
	 * @throws IllegalArgumentException when the action is unknown, not enabled, or
	 *                                  names no message
	 */
	@Override
	protected Object executeMailAction(String action, Map<String, Object> parameters) {
		String folder = resolveFolderName(getParameterValue(parameters, FOLDER_PARAM, null));
		List<String> ids = parameterAsList(parameters, UID_PARAM);
		validateIds(ids);

		MailboxActionResult result;
		String normalizedAction;
		String targetFolder = null;
		if (MARK_READ_ACTION.equalsIgnoreCase(action) || MARK_UNREAD_ACTION.equalsIgnoreCase(action)) {
			requireFlagChanges();
			normalizedAction = MARK_READ_ACTION.equalsIgnoreCase(action) ? MARK_READ_ACTION : MARK_UNREAD_ACTION;
			result = mailboxClient().mark(folder, ids, MARK_READ_ACTION.equals(normalizedAction));
		} else if (MOVE_ACTION.equalsIgnoreCase(action)) {
			targetFolder = getParameterValue(parameters, TARGET_FOLDER_PARAM, null);
			requireMove(targetFolder, folder);
			normalizedAction = MOVE_ACTION;
			result = mailboxClient().move(folder, ids, targetFolder);
		} else if (DELETE_ACTION.equalsIgnoreCase(action)) {
			requireDelete();
			normalizedAction = DELETE_ACTION;
			result = mailboxClient().delete(folder, ids);
		} else {
			throw new IllegalArgumentException(
					"The '" + action + "' action is not available. Available actions are " + availableActions());
		}

		Map<String, Object> output = new LinkedHashMap<>();
		output.put("action", normalizedAction);
		output.put("folder", folder);
		output.put("requested", ids.size());
		output.put("affected", result.affected());
		output.put("uids", result.messageIds());
		if (targetFolder != null) {
			output.put(TARGET_FOLDER_PARAM, targetFolder);
		}
		return output;
	}

	/**
	 * Mark messages read or unread, for a caller holding the engine rather than
	 * going through the function parameters.
	 *
	 * @param folderName the folder holding them, or null for this engine's default
	 * @param uids       the messages to mark
	 * @param seen       true to mark read, false to mark unread
	 * @return how many were marked
	 * @throws IllegalArgumentException when this engine may not change read state
	 */
	public int markMessages(String folderName, List<Long> uids, boolean seen) {
		requireFlagChanges();
		validateIds(uids.stream().map(String::valueOf).toList());
		return mailboxClient().mark(resolveFolderName(folderName), uids.stream().map(String::valueOf).toList(), seen)
				.affected();
	}

	/**
	 * Move messages to another folder, for a caller holding the engine rather than
	 * going through the function parameters.
	 *
	 * @param folderName   the folder holding them, or null for this engine's
	 *                     default
	 * @param uids         the messages to move
	 * @param targetFolder the folder to move them to
	 * @return how many moved
	 * @throws IllegalArgumentException when this engine may not move messages, or
	 *                                  may not use that folder
	 */
	public int moveMessages(String folderName, List<Long> uids, String targetFolder) {
		String folder = resolveFolderName(folderName);
		requireMove(targetFolder, folder);
		List<String> ids = uids.stream().map(String::valueOf).toList();
		validateIds(ids);
		return mailboxClient().move(folder, ids, targetFolder).affected();
	}

	/**
	 * Delete messages, for a caller holding the engine rather than going through
	 * the function parameters.
	 *
	 * @param folderName the folder holding them, or null for this engine's default
	 * @param uids       the messages to delete
	 * @return how many were deleted
	 * @throws IllegalArgumentException when this engine may not delete messages
	 */
	public int deleteMessages(String folderName, List<Long> uids) {
		requireDelete();
		List<String> ids = uids.stream().map(String::valueOf).toList();
		validateIds(ids);
		return mailboxClient().delete(resolveFolderName(folderName), ids).affected();
	}

	/**
	 * Check that an action names messages, and not more than this engine will act
	 * on at once.
	 *
	 * <p>
	 * The cap is the same one that bounds a search, and matters more here: a search
	 * that returns too much is only wasteful, where a delete that takes too much is
	 * not undoable.
	 *
	 * @param ids the messages the call named
	 * @throws IllegalArgumentException when there are none, or too many
	 */
	private void validateIds(List<String> ids) {
		if (ids == null || ids.isEmpty()) {
			throw new IllegalArgumentException("Must define the " + UID_PARAM + " parameter to identify a message");
		}
		if (ids.size() > this.maxMessages) {
			throw new IllegalArgumentException("This function engine acts on at most " + this.maxMessages
					+ " messages per call but " + ids.size() + " were provided");
		}
	}

	/**
	 * @throws IllegalArgumentException when this engine may not change read state
	 */
	private void requireFlagChanges() {
		if (!this.allowFlagChanges) {
			throw new IllegalArgumentException("This function engine cannot change read state. Set "
					+ ALLOW_FLAG_CHANGES_KEY + " in the SMSS to allow it");
		}
	}

	/**
	 * Check that a move is allowed, named, and goes somewhere.
	 *
	 * <p>
	 * A move onto the folder the messages are already in is refused rather than
	 * carried out, since on IMAP it is a copy followed by a delete and would leave
	 * the mailbox holding two of everything.
	 *
	 * @param targetFolder the folder to move to, or null when the call named none
	 * @param sourceFolder the folder the messages are in
	 * @throws IllegalArgumentException when this engine may not move messages, the
	 *                                  target is missing or not allowed, or it is
	 *                                  where they already are
	 */
	private void requireMove(String targetFolder, String sourceFolder) {
		if (!this.allowMove) {
			throw new IllegalArgumentException(
					"This function engine cannot move messages. Set " + ALLOW_MOVE_KEY + " in the SMSS to allow it");
		}
		if (targetFolder == null) {
			throw new IllegalArgumentException("Must define " + TARGET_FOLDER_PARAM + " to move a message");
		}
		if (!isAllowedFolder(targetFolder)) {
			throw new IllegalArgumentException("This function engine can only use folders " + this.allowedFolders);
		}
		if (targetFolder.equalsIgnoreCase(sourceFolder)) {
			throw new IllegalArgumentException("The messages are already in " + sourceFolder);
		}
	}

	/**
	 * @throws IllegalArgumentException when this engine may not delete messages
	 */
	private void requireDelete() {
		if (!this.allowDelete) {
			throw new IllegalArgumentException("This function engine cannot delete messages. Set " + ALLOW_DELETE_KEY
					+ " in the SMSS to allow it");
		}
	}

	/**
	 * @return whether this engine can alter the mailbox at all, which includes
	 *         marking what it reads, since that changes the mailbox for everyone
	 *         else looking at it
	 */
	protected boolean changesTheMailbox() {
		return this.allowFlagChanges || this.allowMove || this.allowDelete || this.markAsRead;
	}

	/**
	 * @return the actions this engine has been allowed, which is what it publishes
	 *         and what an unknown action is reported against
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
	 * @param folder the folder to check
	 * @return whether this engine may use it, which every folder is when no
	 *         allowlist was configured
	 */
	private boolean isAllowedFolder(String folder) {
		return folder != null && (this.allowedFolders.isEmpty()
				|| this.allowedFolders.stream().anyMatch(allowed -> allowed.equalsIgnoreCase(folder)));
	}

	/**
	 * Work out which folder to read, and refuse one this engine may not use.
	 *
	 * <p>
	 * Unlike the single folder protocols, which warn and read the inbox anyway,
	 * this refuses outright: IMAP does have the folder that was asked for, so
	 * quietly answering from a different one would be misleading.
	 */
	@Override
	protected String resolveFolderName(String requestedFolder) {
		String folder = requestedFolder == null ? this.defaultFolder : requestedFolder;
		if (!isAllowedFolder(folder)) {
			throw new IllegalArgumentException("This function engine can only use folders " + this.allowedFolders
					+ " but " + folder + " was requested");
		}
		return folder;
	}

	@Override
	protected void addProtocolParameters(List<FunctionParameter> parameters) {
		List<String> actions = availableActions();
		if (!actions.isEmpty()) {
			parameters.add(new FunctionParameter(ACTION_PARAM, "string",
					"Optional. Defaults to search. Other actions: " + String.join(", ", actions) + "."));
			parameters.add(new FunctionParameter(UID_PARAM, "string",
					"Message uid, or comma separated uids, for an action other than search."));
			if (this.allowMove) {
				parameters.add(new FunctionParameter(TARGET_FOLDER_PARAM, "string",
						"Folder to move the message to." + allowedFolderText()));
			}
		}
		parameters.add(new FunctionParameter(FOLDER_PARAM, "string",
				"Optional mailbox folder." + defaultText(this.defaultFolder) + allowedFolderText()));
		parameters.add(new FunctionParameter(UNREAD_ONLY_PARAM, "boolean",
				"Optional. Set true to return only unread messages. Defaults to false."));
	}

	/**
	 * @return a sentence naming the folders this engine may use, so a caller
	 *         reading the parameter knows before trying, or empty when any folder
	 *         is allowed
	 */
	private String allowedFolderText() {
		return this.allowedFolders.isEmpty() ? ""
				: " Only these folders can be used: " + String.join(", ", this.allowedFolders) + ".";
	}

	@Override
	protected String getDefaultFunctionDescription() {
		String description = "Read email in a mailbox with folders, server-side search, and read state.";
		List<String> actions = availableActions();
		return actions.isEmpty() ? description
				: description + " Allowed mailbox-changing actions: " + String.join(", ", actions) + ".";
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
}
