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
package prerna.engine.impl.function.mail.adapter.jakarta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Address;
import jakarta.mail.AuthenticationFailedException;
import jakarta.mail.FetchProfile;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Store;
import prerna.engine.impl.function.mail.adapter.jakarta.auth.MailStoreAuthentication;
import prerna.engine.impl.function.mail.attachment.AttachmentStore;
import prerna.engine.impl.function.mail.config.JakartaStoreConfig;
import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.model.MailSearchCriteria;
import prerna.engine.impl.function.mail.model.MailSearchRequest;
import prerna.engine.impl.function.mail.model.MailSearchResult;
import prerna.engine.impl.function.mail.policy.MailReadPolicy;
import prerna.engine.impl.function.mail.spi.MailboxClient;

/**
 * Everything reading a mailbox over a protocol involves, apart from the
 * protocol itself.
 *
 * <p>
 * IMAP and POP3 differ in how messages are found and in what may be done to
 * them afterwards, and in nothing else. Building the session, connecting,
 * retrying a refused sign in, opening a folder, walking it newest first and
 * turning each message into a map is the same either way, and is here.
 *
 * <p>
 * A subclass supplies the search, and says what its protocol can do. The
 * defaults are POP3's answers - no flags, no uids, nothing to do after reading
 * a message - because those are the ones that are true of a bare mail store,
 * and IMAP is the one with more to say.
 * 
 * <p>
 * TLS is on and verified unless the SMSS turns it off, and a starttls
 * connection requires the upgrade rather than continuing in plain text if the
 * server declines. That is stricter than jakarta.mail's own defaults, which
 * quietly carry on unencrypted.
 */
public abstract class AbstractJakartaMailboxClient implements MailboxClient {

	private static final Logger classLogger = LogManager.getLogger(AbstractJakartaMailboxClient.class);

	protected final JakartaStoreConfig config;
	protected final MailReadPolicy policy;
	private final MailStoreAuthentication authentication;
	private final JakartaMessageMapper messageMapper;
	private final Session session;

	private Store store;

	/**
	 * @param config          which mail server to reach and how
	 * @param authentication  how to sign in to it
	 * @param policy          how much of the mailbox may be returned
	 * @param attachmentStore where an attachment is written when one is asked for
	 */
	protected AbstractJakartaMailboxClient(JakartaStoreConfig config, MailStoreAuthentication authentication,
			MailReadPolicy policy, AttachmentStore attachmentStore) {
		this.config = config;
		this.authentication = authentication;
		this.policy = policy;
		this.messageMapper = new JakartaMessageMapper(policy, attachmentStore);
		this.session = buildSession();
	}

	/**
	 * Find messages and describe them.
	 *
	 * <p>
	 * The folder is walked backwards because a mail store keeps messages oldest
	 * first and callers want the newest, and it stops as soon as the limit is
	 * reached rather than reading the whole folder and discarding most of it.
	 *
	 * <p>
	 * A message that cannot be read is counted and skipped rather than failing the
	 * search. One malformed message in a mailbox is common, and it should not cost
	 * the caller the ones either side of it.
	 */
	@Override
	public MailSearchResult search(MailSearchRequest request) {
		Folder folder = null;
		try {
			folder = openFolder(request.folder(), folderOpenMode());
			Message[] found = findMessages(folder, request.criteria());
			List<java.util.Map<String, Object>> messages = new ArrayList<>();
			int unreadable = 0;
			if (found != null && found.length > 0) {
				int windowStart = Math.max(0, found.length - ((request.limit() * 3) + 10));
				prefetch(folder, Arrays.copyOfRange(found, windowStart, found.length));
				for (int i = found.length - 1; i >= 0 && messages.size() < request.limit(); i--) {
					Message message = found[i];
					try {
						if (!senderAllowed(message)) {
							continue;
						}
						messages.add(this.messageMapper.toMap(folder, message, supportsFlags(), this::messageUid,
								request.includeBody(), request.downloadAttachments(), request.insight()));
						afterMessageRead(folder, message);
					} catch (MessagingException | IOException e) {
						classLogger.warn("Skipping a message in " + request.folder() + " that could not be read", e);
						unreadable++;
					}
				}
			}
			return new MailSearchResult(request.folder(), messages, unreadable);
		} catch (MessagingException e) {
			classLogger.error("Error reading the {} folder of {}", request.folder(), this.config.host(), e);
			throw new IllegalArgumentException("Error occurred reading the mailbox. Detailed error: " + e.getMessage(),
					e);
		} finally {
			closeFolder(folder, false);
		}
	}

	/**
	 * Find the messages that match, in whatever way this protocol can.
	 *
	 * <p>
	 * IMAP asks the server; POP3 has no search at all and returns everything for
	 * the caller to sift.
	 *
	 * @param folder   the open folder to search
	 * @param criteria what to match on
	 * @return the matching messages, oldest first, or null when there are none
	 * @throws MessagingException when the folder cannot be searched
	 */
	protected abstract Message[] findMessages(Folder folder, MailSearchCriteria criteria) throws MessagingException;

	/**
	 * @return how the folder is opened, which is read only unless a subclass has
	 *         something to write
	 */
	protected int folderOpenMode() {
		return Folder.READ_ONLY;
	}

	/**
	 * Do whatever reading a message implies for this protocol, such as marking it
	 * read.
	 *
	 * @param folder  the folder the message is in
	 * @param message the message that was just read
	 * @throws MessagingException when the message cannot be changed
	 */
	protected void afterMessageRead(Folder folder, Message message) throws MessagingException {
		// reading a message changes nothing unless a subclass says otherwise
	}

	/**
	 * The id that will still find this message on a later call.
	 *
	 * @param folder  the folder the message is in
	 * @param message the message to name
	 * @return the id, or null when the protocol has no id that outlives the
	 *         connection
	 * @throws MessagingException when the id cannot be read
	 */
	protected Object messageUid(Folder folder, Message message) throws MessagingException {
		return null;
	}

	/**
	 * @return whether this protocol records what has been read, which POP3 does not
	 */
	protected boolean supportsFlags() {
		return false;
	}

	/**
	 * Open a folder by name.
	 *
	 * <p>
	 * A missing folder is refused by name rather than left to fail obscurely later,
	 * since the usual cause is a folder that is spelled differently on the server.
	 *
	 * @param folderName the folder to open
	 * @param mode       how to open it
	 * @return the open folder
	 * @throws MessagingException       when it cannot be opened
	 * @throws IllegalArgumentException when the mailbox has no such folder
	 */
	protected Folder openFolder(String folderName, int mode) throws MessagingException {
		Folder folder = connectedStore().getFolder(folderName);
		if (folder == null || !folder.exists()) {
			throw new IllegalArgumentException("The mailbox does not have a folder named " + folderName);
		}
		folder.open(mode);
		return folder;
	}

	/**
	 * Close a folder, if it is open.
	 *
	 * <p>
	 * A failure to close is logged rather than thrown, because it would otherwise
	 * replace whatever the caller was actually doing - including the error that
	 * made them stop.
	 *
	 * @param folder  the folder to close, which may be null
	 * @param expunge whether deletions are carried out on the way out
	 */
	protected void closeFolder(Folder folder, boolean expunge) {
		if (folder == null || !folder.isOpen()) {
			return;
		}
		try {
			folder.close(expunge);
		} catch (MessagingException e) {
			classLogger.warn("Error closing the " + folder.getName() + " folder", e);
		}
	}

	/**
	 * Whether this message came from somewhere the engine accepts.
	 *
	 * <p>
	 * One acceptable sender is enough, and a message with no sender at all is
	 * refused, since an unattributable message is what an allowlist is for.
	 *
	 * @param message the message to check
	 * @return true when it may be returned
	 * @throws MessagingException when the sender cannot be read
	 */
	private boolean senderAllowed(Message message) throws MessagingException {
		if (this.policy.allowedSenderDomains().isEmpty()) {
			return true;
		}
		Address[] senders = message.getFrom();
		if (senders == null) {
			return false;
		}
		for (Address sender : senders) {
			if (sender != null && this.policy.isSenderAllowed(sender.toString())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Ask the server for the headers of the messages about to be read, in one go.
	 *
	 * <p>
	 * Without this, every header of every message is a separate round trip, which
	 * is the difference between a search that answers and one that appears to hang.
	 * Only the end of the folder is prefetched, since that is the part a newest
	 * first walk reaches, with room for the ones the allowlist will skip.
	 *
	 * <p>
	 * A failure here costs speed rather than correctness, so it is logged and the
	 * search goes on.
	 *
	 * @param folder   the open folder
	 * @param messages the messages likely to be read
	 */
	private void prefetch(Folder folder, Message[] messages) {
		try {
			FetchProfile profile = new FetchProfile();
			profile.add(FetchProfile.Item.ENVELOPE);
			if (supportsFlags()) {
				profile.add(FetchProfile.Item.FLAGS);
			}
			folder.fetch(messages, profile);
		} catch (MessagingException e) {
			classLogger.warn("Could not prefetch message headers from " + this.config.host(), e);
		}
	}

	/**
	 * Build the mail session this client connects through.
	 *
	 * <p>
	 * The order matters and is deliberate: the settings this class derives are put
	 * down first, then the authentication adds its own, and the raw {@code mail.}
	 * keys out of the SMSS go on last so they win. That is the passthrough working
	 * as intended - no set of named keys covers every mail server, and somebody
	 * facing one this class does not model can still reach it.
	 *
	 * <p>
	 * {@code ONLY_CUSTOM_PROPS} skips the derived settings entirely, for a server
	 * that needs a combination this would otherwise interfere with.
	 *
	 * @return the session
	 */
	private Session buildSession() {
		Properties mailProperties = new Properties();
		mailProperties.setProperty(MailProperties.STORE_PROTOCOL, this.config.storeProtocol());
		mailProperties.setProperty(MailProperties.rawProperty(this.config.storeProtocol(), "host"), this.config.host());
		mailProperties.setProperty(MailProperties.rawProperty(this.config.storeProtocol(), "port"),
				Integer.toString(this.config.port()));

		if (!this.config.onlyCustomProperties()) {
			if (!MailProperties.NO_SECURITY.equals(this.config.security())) {
				if (this.config.secureProtocolSelected()) {
					mailProperties.setProperty(MailProperties.rawProperty(this.config.storeProtocol(), "ssl.enable"),
							"true");
				} else {
					mailProperties.setProperty(
							MailProperties.rawProperty(this.config.storeProtocol(), "starttls.enable"), "true");
					mailProperties.setProperty(
							MailProperties.rawProperty(this.config.storeProtocol(), "starttls.required"), "true");
				}
				mailProperties.setProperty(
						MailProperties.rawProperty(this.config.storeProtocol(), "ssl.checkserveridentity"), "true");
				mailProperties.setProperty(MailProperties.rawProperty(this.config.storeProtocol(), "ssl.protocols"),
						"TLSv1.2 TLSv1.3");
			}
			mailProperties.setProperty(MailProperties.rawProperty(this.config.storeProtocol(), "connectiontimeout"),
					Integer.toString(this.config.connectionTimeout()));
			mailProperties.setProperty(MailProperties.rawProperty(this.config.storeProtocol(), "timeout"),
					Integer.toString(this.config.readTimeout()));
		}

		this.authentication.configure(mailProperties, this.config.storeProtocol());
		for (String key : this.config.sourceProperties().stringPropertyNames()) {
			if (key.startsWith(MailProperties.RAW_MAIL_PROPERTY_PREFIX)) {
				mailProperties.setProperty(key, this.config.sourceProperties().getProperty(key));
			}
		}

		classLogger.info("Creating a mail session against {}:{} over {}, signing in as {} with {}", this.config.host(),
				this.config.port(), this.config.storeProtocol(), this.config.username(),
				this.authentication.description());
		PasswordAuthentication sessionAuthentication = this.authentication
				.sessionAuthentication(this.config.username());
		if (sessionAuthentication == null) {
			return Session.getInstance(mailProperties);
		}
		return Session.getInstance(mailProperties, new jakarta.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return sessionAuthentication;
			}
		});
	}

	/**
	 * The open connection to the mail server, opening or reopening it as needed.
	 *
	 * <p>
	 * Reading holds a live connection and a mail server drops one that has been
	 * idle, so the store is opened on first use and reopened whenever it has gone
	 * away rather than connected once and left to go stale. A store that timed out
	 * reports itself as not connected but will not reconnect, which is why it is
	 * dropped rather than reused.
	 *
	 * <p>
	 * A refused sign in is retried once, but only when the credential is the kind
	 * that can have changed. A password out of an SMSS will not be any different
	 * the second time; a token that expired early will.
	 *
	 * @return the connected store
	 * @throws IllegalArgumentException when the mail server cannot be reached or
	 *                                  refuses the credentials
	 */
	private synchronized Store connectedStore() {
		if (this.store != null && this.store.isConnected()) {
			return this.store;
		}
		closeStore();
		try {
			connect();
		} catch (AuthenticationFailedException e) {
			if (!this.authentication.refreshAfterRejection()) {
				throw connectionError(e);
			}
			classLogger.warn("The {} server {} rejected the credentials; retrying once", this.config.storeProtocol(),
					this.config.host());
			try {
				connect();
			} catch (MessagingException retried) {
				throw connectionError(retried);
			}
		} catch (MessagingException e) {
			throw connectionError(e);
		}
		return this.store;
	}

	/**
	 * Open the connection.
	 *
	 * <p>
	 * The secret is asked for here rather than held, so a credential that expires
	 * hands over a current one on every attempt.
	 *
	 * @throws MessagingException when the server cannot be reached or refuses the
	 *                            credentials
	 */
	private void connect() throws MessagingException {
		this.store = this.session.getStore(this.config.storeProtocol());
		this.store.connect(this.config.host(), this.config.port(), this.config.username(),
				this.authentication.connectSecret());
	}

	/**
	 * Log a failed connection and turn it into the error a caller sees.
	 *
	 * <p>
	 * What the credential itself says goes in the log rather than the error,
	 * because it is for whoever set the engine up and can name things a caller has
	 * no business seeing. The hint is the opposite: it says what to check, and is
	 * added only for a refusal, since a server that could not be reached at all has
	 * nothing to do with the credentials.
	 *
	 * @param error what went wrong
	 * @return the error to throw
	 */
	private IllegalArgumentException connectionError(MessagingException error) {
		String diagnostic = this.authentication.diagnostic();
		if (diagnostic != null) {
			classLogger.error("Mail authentication failed and {}", diagnostic);
		}
		classLogger.error("Error connecting to {}:{}", this.config.host(), this.config.port(), error);
		String message = "Error occurred connecting to the mail server defined. Detailed error: " + error.getMessage();
		if (error instanceof AuthenticationFailedException && this.authentication.failureHint() != null) {
			message += " " + this.authentication.failureHint();
		}
		return new IllegalArgumentException(message, error);
	}

	/**
	 * Drop the connection, whether or not it closes cleanly. The field is cleared
	 * either way, so a store that failed to close is not held on to and reused.
	 */
	private void closeStore() {
		if (this.store == null) {
			return;
		}
		try {
			this.store.close();
		} catch (MessagingException e) {
			classLogger.warn("Error closing the connection to " + this.config.host(), e);
		} finally {
			this.store = null;
		}
	}

	@Override
	public synchronized void close() {
		closeStore();
	}
}
