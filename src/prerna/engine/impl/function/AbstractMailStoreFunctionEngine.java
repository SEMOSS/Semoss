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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;

import jakarta.mail.Address;
import jakarta.mail.AuthenticationFailedException;
import jakarta.mail.FetchProfile;
import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.Part;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Store;
import jakarta.mail.internet.MimeUtility;
import prerna.om.Insight;
import prerna.util.Constants;

/**
 * Shared behavior for the function engines that read a mailbox.
 *
 * <p>
 * POP3 and IMAP differ in what they can be asked - IMAP has folders, tracks
 * which messages have been read, and searches on the server, while POP3 has a
 * single inbox and no notion of either - but everything around that is the same
 * work: turning a mailbox configuration into a connection, keeping that
 * connection usable, and turning a MIME message into something a caller can
 * read. That all lives here so the two protocols differ only where they
 * actually differ.
 *
 * <p>
 * Reading a mailbox hands whatever is in it to whoever called, so the SMSS
 * carries the limits rather than the caller: how many messages one call can
 * return, how much of a body comes back, which senders are surfaced at all, and
 * whether attachments can be written into the calling insight.
 */
public abstract class AbstractMailStoreFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractMailStoreFunctionEngine.class);

	// the connection keys are protocol specific, so each engine spells its own out
	// of these suffixes - POP3_HOST, IMAP_HOST, and so on
	protected static final String HOST_SUFFIX = "HOST";
	protected static final String PORT_SUFFIX = "PORT";
	protected static final String USERNAME_SUFFIX = "USERNAME";
	protected static final String PASSWORD_SUFFIX = "PASSWORD";
	protected static final String SECURITY_SUFFIX = "SECURITY";

	// public so a caller building an engine in memory rather than from a
	// cataloged SMSS can populate the properties by name
	public static final String ONLY_CUSTOM_PROPS_KEY = "ONLY_CUSTOM_PROPS";
	public static final String CONNECTION_TIMEOUT_KEY = "CONNECTION_TIMEOUT";
	public static final String READ_TIMEOUT_KEY = "READ_TIMEOUT";
	public static final String MAX_MESSAGES_KEY = "MAX_MESSAGES";
	public static final String DEFAULT_MESSAGES_KEY = "DEFAULT_MESSAGES";
	public static final String MAX_BODY_CHARS_KEY = "MAX_BODY_CHARS";
	public static final String ALLOWED_SENDER_DOMAINS_KEY = "ALLOWED_SENDER_DOMAINS";
	public static final String ALLOW_ATTACHMENT_DOWNLOAD_KEY = "ALLOW_ATTACHMENT_DOWNLOAD";
	public static final String MAX_ATTACHMENT_SIZE_KEY = "MAX_ATTACHMENT_SIZE";

	// a key starting with this is copied onto the mail session verbatim, so a
	// server needing a jakarta.mail property this engine does not model can still
	// be configured without a code change
	public static final String RAW_MAIL_PROPERTY_PREFIX = "mail.";
	public static final String STORE_PROTOCOL_PROPERTY = "mail.store.protocol";

	// how the connection is secured. ssl opens an encrypted socket directly,
	// starttls upgrades a plaintext connection, and none is only reasonable for an
	// internal server that does no TLS at all
	public static final String SSL_SECURITY = "ssl";
	public static final String STARTTLS_SECURITY = "starttls";
	public static final String NONE_SECURITY = "none";

	// what execute does when it is not asked for anything else
	public static final String SEARCH_ACTION = "search";

	// parameters execute understands. the protocol specific ones are described by
	// the engine that supports them
	protected static final String ACTION_PARAM = "action";
	protected static final String FOLDER_PARAM = "folder";
	protected static final String LIMIT_PARAM = "limit";
	protected static final String FROM_PARAM = "from";
	protected static final String SUBJECT_PARAM = "subject";
	protected static final String SINCE_DAYS_PARAM = "sinceDays";
	protected static final String UNREAD_ONLY_PARAM = "unreadOnly";
	protected static final String INCLUDE_BODY_PARAM = "includeBody";
	protected static final String DOWNLOAD_ATTACHMENTS_PARAM = "downloadAttachments";

	// the one folder every mail server has
	protected static final String INBOX_FOLDER = "INBOX";

	private static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;
	private static final String TRUNCATION_MARKER = " ... [truncated]";
	private static final int COPY_BUFFER_SIZE = 8192;

	protected String host = null;
	protected String port = null;
	protected String username = null;
	protected String password = null;
	protected String security = SSL_SECURITY;

	// the jakarta.mail protocol the store is opened under, so pop3s rather than
	// pop3 when the connection is encrypted from the first byte
	protected String storeProtocol = null;

	protected int maxMessages = 25;
	protected int defaultMessages = 10;
	protected int maxBodyChars = 10_000;
	protected long maxAttachmentSize = 5L * 1024L * 1024L;
	protected boolean allowAttachmentDownload = false;

	// blank means every sender in the mailbox is surfaced
	protected Set<String> allowedSenderDomains = new LinkedHashSet<>();

	private Session mailSession = null;
	private Store mailStore = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		// read from here on rather than from the argument, so a raw jakarta.mail key
		// is found whichever case it arrived in
		smssProp = normalizeRawMailKeys(smssProp);

		this.security = StringUtils
				.defaultIfEmpty(trimToNull(smssProp.getProperty(key(SECURITY_SUFFIX))), this.security).toLowerCase();
		if (!this.security.equals(SSL_SECURITY) && !this.security.equals(STARTTLS_SECURITY)
				&& !this.security.equals(NONE_SECURITY)) {
			throw new IllegalArgumentException(getClass().getSimpleName() + " only supports " + SSL_SECURITY + ", "
					+ STARTTLS_SECURITY + ", or " + NONE_SECURITY + " for the " + key(SECURITY_SUFFIX) + " key");
		}

		// the protocol is resolved before the host because a configuration written
		// purely in jakarta.mail keys names the host under the protocol it uses
		this.storeProtocol = StringUtils.defaultIfEmpty(trimToNull(smssProp.getProperty(STORE_PROTOCOL_PROPERTY)),
				this.security.equals(SSL_SECURITY) ? getSecureProtocol() : getProtocol());

		this.host = firstNonNull(trimToNull(smssProp.getProperty(key(HOST_SUFFIX))),
				trimToNull(smssProp.getProperty(rawProperty(this.storeProtocol, "host"))),
				trimToNull(smssProp.getProperty(rawProperty(getProtocol(), "host"))),
				trimToNull(smssProp.getProperty(rawProperty(getSecureProtocol(), "host"))), getDefaultHost());
		if (this.host == null) {
			throw new IllegalArgumentException("Must have key " + key(HOST_SUFFIX) + " or "
					+ rawProperty(this.storeProtocol, "host") + " in SMSS to know which mail server to read");
		}

		this.port = firstNonNull(trimToNull(smssProp.getProperty(key(PORT_SUFFIX))),
				trimToNull(smssProp.getProperty(rawProperty(this.storeProtocol, "port"))),
				getDefaultPort(isSecureProtocol()));

		this.username = trimToNull(smssProp.getProperty(key(USERNAME_SUFFIX)));
		this.password = trimToNull(smssProp.getProperty(key(PASSWORD_SUFFIX)));
		// a mail server that lets an anonymous caller read a mailbox does not exist
		// in practice, so a missing credential is a misconfiguration worth failing on
		// rather than connecting and getting a login error on the first read
		if (this.username == null) {
			throw new IllegalArgumentException(
					"Must define " + key(USERNAME_SUFFIX) + " in SMSS to know which mailbox to open");
		}
		if (requiresPassword() && this.password == null) {
			throw new IllegalArgumentException(
					"Must define " + key(PASSWORD_SUFFIX) + " in SMSS to sign in to the mailbox");
		}

		this.maxMessages = Math.max(1, NumberUtils.toInt(smssProp.getProperty(MAX_MESSAGES_KEY), this.maxMessages));
		this.defaultMessages = Math.min(this.maxMessages,
				Math.max(1, NumberUtils.toInt(smssProp.getProperty(DEFAULT_MESSAGES_KEY), this.defaultMessages)));
		this.maxBodyChars = Math.max(100,
				NumberUtils.toInt(smssProp.getProperty(MAX_BODY_CHARS_KEY), this.maxBodyChars));
		this.maxAttachmentSize = Math.max(1024L,
				NumberUtils.toLong(smssProp.getProperty(MAX_ATTACHMENT_SIZE_KEY), this.maxAttachmentSize));
		this.allowAttachmentDownload = parseBoolean(smssProp.getProperty(ALLOW_ATTACHMENT_DOWNLOAD_KEY),
				this.allowAttachmentDownload);

		for (String domain : splitList(smssProp.getProperty(ALLOWED_SENDER_DOMAINS_KEY))) {
			// stored bare so both "@blah.org" and "blah.org" are the same
			this.allowedSenderDomains.add(domain.toLowerCase().replaceFirst("^@", ""));
		}

		openProtocolProperties(smssProp);

		this.mailSession = buildMailSession(smssProp);

		// the SMSS does not have to spell out the function metadata since we know
		// what execute supports. anything defined in the SMSS wins
		setDefaultFunctionMetadata();
	}

	/**
	 * Read the configuration only one protocol has, such as the folders an IMAP
	 * engine is allowed to open. Called while the engine is opening, after the
	 * connection settings have been read and before the session is built.
	 *
	 * @param smssProp the engine properties
	 */
	protected void openProtocolProperties(Properties smssProp) {
		// nothing by default
	}

	/**
	 * Build the mail session this engine reads through.
	 *
	 * <p>
	 * The defaults are deliberately strict: the connection is encrypted, the server
	 * certificate has to match the host, and the protocol floor is TLS 1.2. A
	 * mailbox credential crosses this connection on every read, so a downgrade to
	 * plaintext would put it on the wire in the clear.
	 *
	 * <p>
	 * Anything the caller spelled out as a raw {@code mail.} property is layered on
	 * last and wins, and {@link #ONLY_CUSTOM_PROPS_KEY} skips the defaults entirely
	 * for a server that needs to be described purely in jakarta.mail terms.
	 *
	 * @param smssProp the engine properties
	 * @return the session to read every message through
	 */
	private Session buildMailSession(Properties smssProp) {
		boolean onlyCustomProps = parseBoolean(smssProp.getProperty(ONLY_CUSTOM_PROPS_KEY), false);

		Properties mailProps = new Properties();
		mailProps.setProperty(STORE_PROTOCOL_PROPERTY, this.storeProtocol);
		mailProps.setProperty(rawProperty(this.storeProtocol, "host"), this.host);
		mailProps.setProperty(rawProperty(this.storeProtocol, "port"), this.port);

		if (!onlyCustomProps) {
			if (!this.security.equals(NONE_SECURITY)) {
				if (isSecureProtocol()) {
					// the protocol opens an encrypted socket itself, so there is nothing
					// to upgrade. no socketFactory.class either, since naming one takes
					// the connection off the path where jakarta mail applies
					// checkserveridentity
					mailProps.setProperty(rawProperty(this.storeProtocol, "ssl.enable"), "true");
				} else {
					// required, not just enabled, so a server that quietly drops starttls
					// cannot leave the mailbox credential on the wire in the clear
					mailProps.setProperty(rawProperty(this.storeProtocol, "starttls.enable"), "true");
					mailProps.setProperty(rawProperty(this.storeProtocol, "starttls.required"), "true");
				}
				// for no man-in-the-middle attacks
				mailProps.setProperty(rawProperty(this.storeProtocol, "ssl.checkserveridentity"), "true");
				mailProps.setProperty(rawProperty(this.storeProtocol, "ssl.protocols"), "TLSv1.2 TLSv1.3");
			}

			mailProps.setProperty(rawProperty(this.storeProtocol, "connectiontimeout"),
					Integer.toString(NumberUtils.toInt(smssProp.getProperty(CONNECTION_TIMEOUT_KEY), 10_000)));
			mailProps.setProperty(rawProperty(this.storeProtocol, "timeout"),
					Integer.toString(NumberUtils.toInt(smssProp.getProperty(READ_TIMEOUT_KEY), 30_000)));
		}

		// how this engine signs in, for a mail server that does not take a password
		addAuthenticationProperties(mailProps);

		// applied last so a raw mail. key wins over anything above
		for (String propKey : smssProp.stringPropertyNames()) {
			if (propKey.startsWith(RAW_MAIL_PROPERTY_PREFIX)) {
				mailProps.setProperty(propKey, smssProp.getProperty(propKey));
			}
		}

		classLogger.info("Creating a mail session against {}:{} over {}, signing in as {} with {}", this.host,
				this.port, this.storeProtocol, this.username, getCredentialDescription());

		final String authUsername = this.username;
		final String authPassword = this.password;
		if (authPassword == null) {
			// an engine that signs in with something other than a password hands its
			// credential to the connect call itself, and an authenticator holding a
			// null password would only be there to be handed out by mistake
			return Session.getInstance(mailProps);
		}
		return Session.getInstance(mailProps, new jakarta.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(authUsername, authPassword);
			}
		});
	}

	/**
	 * The connected mailbox this engine reads
	 *
	 * <p>
	 * Reading holds a live connection and a mail server drops one that has been
	 * idle, so the store is opened on first use and reopened whenever it has gone
	 * away rather than connected once and left to go stale.
	 *
	 * @return the connected store
	 */
	protected synchronized Store getConnectedStore() {
		if (this.mailStore != null && this.mailStore.isConnected()) {
			return this.mailStore;
		}
		if (this.mailStore != null) {
			// a store that timed out while idle reports itself as not connected but
			// will not reconnect, so drop it and open a new one
			closeStore();
		}
		try {
			connectStore();
		} catch (AuthenticationFailedException e) {
			// a credential can stop working before it was due to, so the sign in is
			// tried once more with a newly obtained one rather than failing the call
			if (!refreshCredentials()) {
				throw connectionError(e);
			}
			classLogger.warn("The {} server {} rejected the credentials, retrying with a new one", this.storeProtocol,
					this.host);
			try {
				connectStore();
			} catch (MessagingException retried) {
				throw connectionError(retried);
			}
		} catch (MessagingException e) {
			throw connectionError(e);
		}
		return this.mailStore;
	}

	/**
	 * Open the connection to the mail server.
	 *
	 * @throws MessagingException when the mail server cannot be reached or refuses
	 *                            the credentials
	 */
	private void connectStore() throws MessagingException {
		this.mailStore = this.mailSession.getStore(this.storeProtocol);
		this.mailStore.connect(this.host, NumberUtils.toInt(this.port, -1), getConnectUsername(), getConnectPassword());
	}

	/**
	 * Log a failed connection and turn it into the error a caller sees.
	 *
	 * @param e what the mail server said
	 * @return the exception to throw
	 */
	private IllegalArgumentException connectionError(MessagingException e) {
		classLogger.error("Error connecting to the {} server {}:{}", this.storeProtocol, this.host, this.port, e);
		String message = "Error occurred connecting to the mail server defined. Detailed error: " + e.getMessage();
		if (e instanceof AuthenticationFailedException) {
			String hint = getAuthenticationHint();
			if (hint != null && !hint.isEmpty()) {
				message = message + " " + hint;
			}
		}
		return new IllegalArgumentException(message, e);
	}

	/**
	 * What to add to the error when the mail server refuses the sign in, for a way
	 * of signing in that has more than one thing to check.
	 *
	 * @return the sentence to append, or null when there is nothing to add
	 */
	protected String getAuthenticationHint() {
		return null;
	}

	/**
	 * Find messages and return them.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @return a map describing what was found, so the caller can tell an empty
	 *         mailbox from a search that matched nothing
	 */
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		if (parameterValues == null) {
			parameterValues = new HashMap<>();
		}
		Insight executingInsight = (Insight) parameterValues.remove(Constants.INSIGHT);

		validateRequiredParameters(parameterValues);

		String action = getParameterValue(parameterValues, ACTION_PARAM, SEARCH_ACTION);
		if (SEARCH_ACTION.equalsIgnoreCase(action)) {
			return searchMessages(parameterValues, executingInsight);
		}
		return executeMailAction(action, parameterValues);
	}

	/**
	 * Do something to the mailbox other than read it. Only implemented by a
	 * protocol that can, and only for the changes an admin turned on.
	 *
	 * @param action          the action the caller asked for
	 * @param parameterValues the runtime parameters for this call
	 * @return a map describing what changed
	 */
	protected Object executeMailAction(String action, Map<String, Object> parameterValues) {
		throw new IllegalArgumentException("The '" + action
				+ "' action is not something this function engine can do. The only action available is "
				+ SEARCH_ACTION);
	}

	/**
	 * Run one search and turn the matches into maps.
	 *
	 * <p>
	 * The messages are read newest first, since a caller asking for the last few
	 * messages means the most recent ones, and the conversion happens while the
	 * folder is still open because a message cannot be read after it closes.
	 *
	 * @param parameterValues  the runtime parameters for this call
	 * @param executingInsight the insight this call is running under, or null
	 * @return the search output
	 */
	protected Map<String, Object> searchMessages(Map<String, Object> parameterValues, Insight executingInsight) {
		String folderName = resolveFolderName(getParameterValue(parameterValues, FOLDER_PARAM, null));

		int limit = getIntParameterValue(parameterValues, LIMIT_PARAM, this.defaultMessages);
		if (limit < 1) {
			limit = 1;
		}
		if (limit > this.maxMessages) {
			classLogger.warn(
					"A limit of {} was passed in but this function engine returns at most {} messages per call", limit,
					this.maxMessages);
			limit = this.maxMessages;
		}

		MailSearchCriteria criteria = new MailSearchCriteria();
		criteria.from = getParameterValue(parameterValues, FROM_PARAM, null);
		criteria.subject = getParameterValue(parameterValues, SUBJECT_PARAM, null);
		int sinceDays = getIntParameterValue(parameterValues, SINCE_DAYS_PARAM, -1);
		if (sinceDays > 0) {
			criteria.since = new Date(System.currentTimeMillis() - (sinceDays * MILLIS_PER_DAY));
		}
		criteria.unreadOnly = getBooleanParameterValue(parameterValues, UNREAD_ONLY_PARAM, false);

		boolean includeBody = getBooleanParameterValue(parameterValues, INCLUDE_BODY_PARAM, true);
		boolean downloadAttachments = getBooleanParameterValue(parameterValues, DOWNLOAD_ATTACHMENTS_PARAM, false);
		if (downloadAttachments && !this.allowAttachmentDownload) {
			classLogger.warn("Attachment download was requested but {} is not enabled, returning the names only",
					ALLOW_ATTACHMENT_DOWNLOAD_KEY);
			downloadAttachments = false;
		}

		Folder folder = null;
		try {
			folder = openFolder(folderName, getFolderOpenMode());
			Message[] found = findMessages(folder, criteria);

			List<Map<String, Object>> messages = new ArrayList<>();
			int unreadable = 0;
			if (found != null && found.length > 0) {
				// only the tail of the folder is read, so only the tail has its headers
				// pulled up front. answering for the last few messages of a mailbox of
				// thousands would otherwise cost thousands of envelopes. the window is
				// wider than the limit to leave room for what the sender filter drops,
				// and anything past it still reads a message at a time
				int windowStart = Math.max(0, found.length - ((limit * 3) + 10));
				prefetchHeaders(folder, Arrays.copyOfRange(found, windowStart, found.length));
				// a mail folder hands back its oldest message first
				for (int i = found.length - 1; i >= 0 && messages.size() < limit; i--) {
					Message message = found[i];
					try {
						if (!isSenderAllowed(message)) {
							continue;
						}
						messages.add(toMessageMap(folder, message, includeBody, downloadAttachments, executingInsight));
						afterMessageRead(folder, message);
					} catch (MessagingException | IOException e) {
						// one malformed message in a mailbox should not cost the caller
						// every other message it asked for
						classLogger.warn("Skipping a message in " + folderName + " that could not be read", e);
						unreadable++;
					}
				}
			}

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("folder", folderName);
			output.put("count", messages.size());
			output.put("messages", messages);
			if (unreadable > 0) {
				output.put("unreadable", unreadable);
			}
			return output;
		} catch (MessagingException e) {
			classLogger.error("Error reading the " + folderName + " folder of " + this.host, e);
			throw new IllegalArgumentException("Error occurred reading the mailbox. Detailed error: " + e.getMessage(),
					e);
		} finally {
			closeFolder(folder, false);
		}
	}

	/**
	 * Find the messages matching one search. IMAP asks the server, POP3 has to walk
	 * the mailbox itself.
	 *
	 * @param folder   the open folder to search
	 * @param criteria what the caller asked to match on
	 * @return the matches, oldest first
	 * @throws MessagingException when the mail server refuses the search
	 */
	protected abstract Message[] findMessages(Folder folder, MailSearchCriteria criteria) throws MessagingException;

	/**
	 * Open a folder of this mailbox.
	 *
	 * @param folderName the folder to open
	 * @param mode       {@link Folder#READ_ONLY} or {@link Folder#READ_WRITE}
	 * @return the open folder
	 * @throws MessagingException when the folder cannot be opened
	 */
	protected Folder openFolder(String folderName, int mode) throws MessagingException {
		Folder folder = getConnectedStore().getFolder(folderName);
		if (folder == null || !folder.exists()) {
			throw new IllegalArgumentException("The mailbox does not have a folder named " + folderName);
		}
		folder.open(mode);
		return folder;
	}

	/**
	 * Close a folder, if it was ever opened.
	 *
	 * @param folder  the folder to close, possibly null
	 * @param expunge whether messages flagged as deleted are removed on the way out
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
	 * Pull the headers of every match in one round trip instead of one per message.
	 * Only an optimization - every field is still readable a message at a time - so
	 * a server that will not do it is logged rather than failed.
	 *
	 * @param folder   the open folder
	 * @param messages the matches about to be read
	 */
	private void prefetchHeaders(Folder folder, Message[] messages) {
		try {
			FetchProfile profile = new FetchProfile();
			profile.add(FetchProfile.Item.ENVELOPE);
			if (supportsFlags()) {
				profile.add(FetchProfile.Item.FLAGS);
			}
			folder.fetch(messages, profile);
		} catch (MessagingException e) {
			classLogger.warn("Could not prefetch the message headers from " + this.host, e);
		}
	}

	/**
	 * Turn one message into the map a caller reads.
	 *
	 * @param folder              the open folder the message is in
	 * @param message             the message to convert
	 * @param includeBody         whether the body is read as well as the headers
	 * @param downloadAttachments whether attachments are written into the insight
	 * @param executingInsight    the insight this call is running under, or null
	 * @return the message as a map
	 * @throws MessagingException when the message cannot be read
	 * @throws IOException        when the body or an attachment cannot be read
	 */
	protected Map<String, Object> toMessageMap(Folder folder, Message message, boolean includeBody,
			boolean downloadAttachments, Insight executingInsight) throws MessagingException, IOException {
		Map<String, Object> output = new LinkedHashMap<>();
		putIfPresent(output, "uid", getMessageUid(folder, message));
		putIfPresent(output, "messageId", firstHeader(message, "Message-ID"));
		putIfPresent(output, "from", joinAddresses(message.getFrom()));
		putIfPresent(output, "to", joinAddresses(message.getRecipients(Message.RecipientType.TO)));
		putIfPresent(output, "cc", joinAddresses(message.getRecipients(Message.RecipientType.CC)));
		putIfPresent(output, "subject", message.getSubject());
		putIfPresent(output, "sentDate", formatDate(message.getSentDate()));
		putIfPresent(output, "receivedDate", formatDate(message.getReceivedDate()));
		if (supportsFlags()) {
			output.put("unread", !message.isSet(Flags.Flag.SEEN));
		}

		StringBuilder plainBody = new StringBuilder();
		StringBuilder htmlBody = new StringBuilder();
		List<Part> attachmentParts = new ArrayList<>();
		collectParts(message, plainBody, htmlBody, attachmentParts);

		if (includeBody) {
			// a html only message is read as its text, since the markup is noise to
			// whoever asked what the message says
			String body = plainBody.length() > 0 ? plainBody.toString() : Jsoup.parse(htmlBody.toString()).text();
			body = body.trim();
			if (body.length() > this.maxBodyChars) {
				body = body.substring(0, this.maxBodyChars) + TRUNCATION_MARKER;
				output.put("bodyTruncated", true);
			}
			output.put("body", body);
		}

		if (!attachmentParts.isEmpty()) {
			List<Map<String, Object>> attachments = new ArrayList<>();
			for (Part attachmentPart : attachmentParts) {
				attachments.add(describeAttachment(attachmentPart, downloadAttachments, executingInsight));
			}
			output.put("attachments", attachments);
		}
		return output;
	}

	/**
	 * Walk a message into its body text and its attachments. A multipart message
	 * carries the same content more than once - plain text and html - so both are
	 * collected and the caller picks.
	 *
	 * @param part        the message or one part of it
	 * @param plainBody   collects the text/plain content
	 * @param htmlBody    collects the text/html content
	 * @param attachments collects the parts that are files rather than body
	 * @throws MessagingException when the part cannot be read
	 * @throws IOException        when the content of the part cannot be read
	 */
	private void collectParts(Part part, StringBuilder plainBody, StringBuilder htmlBody, List<Part> attachments)
			throws MessagingException, IOException {
		if (part.isMimeType("multipart/*")) {
			Object content = part.getContent();
			if (content instanceof Multipart) {
				Multipart multipart = (Multipart) content;
				for (int i = 0; i < multipart.getCount(); i++) {
					collectParts(multipart.getBodyPart(i), plainBody, htmlBody, attachments);
				}
				return;
			}
		}

		String disposition = part.getDisposition();
		if (Part.ATTACHMENT.equalsIgnoreCase(disposition)
				|| (part.getFileName() != null && !part.isMimeType("text/*"))) {
			attachments.add(part);
			return;
		}

		if (part.isMimeType("text/plain")) {
			plainBody.append(partAsString(part));
		} else if (part.isMimeType("text/html")) {
			htmlBody.append(partAsString(part));
		} else if (part.getFileName() != null) {
			// an inline part that is still a file, such as an embedded image
			attachments.add(part);
		}
	}

	/**
	 * Read a text part as a string. A part whose charset the mail server did not
	 * name arrives as a stream rather than a string, so both are handled.
	 *
	 * @param part the part to read
	 * @return the content of the part
	 * @throws MessagingException when the part cannot be read
	 * @throws IOException        when the content of the part cannot be read
	 */
	private static String partAsString(Part part) throws MessagingException, IOException {
		Object content = part.getContent();
		if (content instanceof String) {
			return (String) content;
		}
		if (content instanceof InputStream) {
			try (InputStream is = (InputStream) content) {
				return IOUtils.toString(is, StandardCharsets.UTF_8);
			}
		}
		return content == null ? "" : content.toString();
	}

	/**
	 * Describe one attachment, and save it into the calling insight when the caller
	 * asked and the engine allows it.
	 *
	 * @param part             the attachment part
	 * @param download         whether the file is written into the insight
	 * @param executingInsight the insight this call is running under, or null
	 * @return the attachment as a map
	 * @throws MessagingException when the part cannot be read
	 * @throws IOException        when the file cannot be written
	 */
	private Map<String, Object> describeAttachment(Part part, boolean download, Insight executingInsight)
			throws MessagingException, IOException {
		String fileName = attachmentFileName(part);

		Map<String, Object> output = new LinkedHashMap<>();
		output.put("name", fileName);
		int size = part.getSize();
		if (size > 0) {
			output.put("size", size);
		}
		putIfPresent(output, "contentType", trimToNull(part.getContentType()));

		if (!download) {
			return output;
		}
		if (executingInsight == null) {
			classLogger.warn("Attachments can only be downloaded from within an insight that holds the files");
			return output;
		}
		File saved = saveAttachment(part, fileName, executingInsight);
		if (saved != null) {
			// the name, not the resolved path, so the caller is not handed the server
			// side layout of the insight folder
			output.put("savedAs", saved.getName());
		}
		return output;
	}

	/**
	 * Work out what to call an attachment on disk. The name comes from the message,
	 * which means it comes from whoever sent it, so it is reduced to a bare file
	 * name of harmless characters before anything opens a stream to it.
	 *
	 * @param part the attachment part
	 * @return a file name that cannot walk out of the folder it is written to
	 * @throws MessagingException when the part cannot be read
	 */
	private static String attachmentFileName(Part part) throws MessagingException {
		String rawName = part.getFileName();
		if (rawName == null) {
			return "attachment";
		}
		try {
			rawName = MimeUtility.decodeText(rawName);
		} catch (UnsupportedEncodingException e) {
			classLogger.warn("Could not decode the attachment name " + rawName, e);
		}
		String fileName = new File(rawName).getName().replaceAll("[^a-zA-Z0-9._-]", "_");
		if (fileName.isEmpty() || fileName.equals(".") || fileName.equals("..")) {
			return "attachment";
		}
		return fileName;
	}

	/**
	 * Write one attachment into the calling insight's own folder.
	 *
	 * @param part             the attachment part
	 * @param fileName         the sanitized name to write it under
	 * @param executingInsight the insight this call is running under
	 * @return the file written, or null when it was refused
	 * @throws IOException        when the file cannot be written
	 * @throws MessagingException when the part cannot be read
	 */
	private File saveAttachment(Part part, String fileName, Insight executingInsight)
			throws IOException, MessagingException {
		File insightFolder = new File(executingInsight.getInsightFolder());
		if (!insightFolder.exists()) {
			insightFolder.mkdirs();
		}
		String insightFolderPath = insightFolder.getCanonicalPath();

		File target = uniqueFile(insightFolder, fileName);
		if (!target.getCanonicalPath().startsWith(insightFolderPath + File.separator)) {
			classLogger.warn("Refusing to write the attachment {} outside of the insight folder", fileName);
			return null;
		}

		long written = 0;
		boolean tooLarge = false;
		try (InputStream is = part.getInputStream(); OutputStream os = Files.newOutputStream(target.toPath())) {
			byte[] buffer = new byte[COPY_BUFFER_SIZE];
			int read = 0;
			while ((read = is.read(buffer)) > -1) {
				written += read;
				if (written > this.maxAttachmentSize) {
					// the size a mail server reports for a part is an estimate, so the
					// cap is enforced on the bytes actually read
					tooLarge = true;
					break;
				}
				os.write(buffer, 0, read);
			}
		}
		if (tooLarge) {
			classLogger.warn("The attachment {} is larger than the {} of {} bytes and was not saved", fileName,
					MAX_ATTACHMENT_SIZE_KEY, this.maxAttachmentSize);
			Files.deleteIfExists(target.toPath());
			return null;
		}
		return target;
	}

	/**
	 * Find a name nothing is written under yet, so a second message carrying the
	 * same attachment name does not overwrite the first.
	 *
	 * @param folder   the folder being written to
	 * @param fileName the name to start from
	 * @return the file to write
	 */
	private static File uniqueFile(File folder, String fileName) {
		File target = new File(folder, fileName);
		if (!target.exists()) {
			return target;
		}
		String base = fileName;
		String extension = "";
		int dot = fileName.lastIndexOf('.');
		if (dot > 0) {
			base = fileName.substring(0, dot);
			extension = fileName.substring(dot);
		}
		for (int i = 1; i < 1000; i++) {
			target = new File(folder, base + "_" + i + extension);
			if (!target.exists()) {
				return target;
			}
		}
		return target;
	}

	/**
	 * Whether a message is surfaced at all. An engine can be pointed at a mailbox
	 * that receives more than the caller should see, so a message from outside the
	 * allowed domains is dropped rather than returned.
	 *
	 * @param message the message to check
	 * @return true when the sender is allowed
	 * @throws MessagingException when the message cannot be read
	 */
	protected boolean isSenderAllowed(Message message) throws MessagingException {
		if (this.allowedSenderDomains.isEmpty()) {
			return true;
		}
		Address[] from = message.getFrom();
		if (from == null || from.length == 0) {
			return false;
		}
		for (Address address : from) {
			if (address == null) {
				continue;
			}
			String value = address.toString().toLowerCase();
			int at = value.lastIndexOf('@');
			if (at < 0) {
				continue;
			}
			// the address may still be wrapped in a display name, as in
			// "Someone <someone@blah.org>"
			String domain = value.substring(at + 1).replaceAll("[>\"\\s]", "");
			for (String allowedDomain : this.allowedSenderDomains) {
				// a subdomain of an allowed domain is allowed as well, so listing
				// semoss.org also covers mail.semoss.org
				if (domain.equals(allowedDomain) || domain.endsWith("." + allowedDomain)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Work out which folder to read. A protocol with only one inbox says so, and
	 * one with folders checks the request against what it is allowed to open.
	 *
	 * @param requestedFolder the folder the caller named, or null
	 * @return the folder to open
	 */
	protected String resolveFolderName(String requestedFolder) {
		if (requestedFolder != null && !requestedFolder.equalsIgnoreCase(INBOX_FOLDER)) {
			classLogger.warn("{} only has an {} folder, reading it rather than {}", getProtocol(), INBOX_FOLDER,
					requestedFolder);
		}
		return INBOX_FOLDER;
	}

	/**
	 * How a folder is opened for a search. Read only unless the engine was told to
	 * mark what it reads.
	 *
	 * @return {@link Folder#READ_ONLY} or {@link Folder#READ_WRITE}
	 */
	protected int getFolderOpenMode() {
		return Folder.READ_ONLY;
	}

	/**
	 * Called once a message has been read, for a protocol that records that it was.
	 *
	 * @param folder  the open folder the message is in
	 * @param message the message that was just read
	 * @throws MessagingException when the message cannot be updated
	 */
	protected void afterMessageRead(Folder folder, Message message) throws MessagingException {
		// nothing by default
	}

	/**
	 * The server side id of a message, which is what a caller passes back to act on
	 * it later. Only a protocol that has stable ids returns one.
	 *
	 * @param folder  the open folder the message is in
	 * @param message the message
	 * @return the uid, or null when the protocol has none
	 * @throws MessagingException when the message cannot be read
	 */
	protected Long getMessageUid(Folder folder, Message message) throws MessagingException {
		return null;
	}

	/**
	 * Whether this protocol tracks which messages have been read.
	 *
	 * @return true when the flags of a message mean something
	 */
	protected boolean supportsFlags() {
		return false;
	}

	/**
	 * The mail server to use when the SMSS does not name one, for a hosted service
	 * that always lives at the same address.
	 *
	 * @return the host, or null when it has to be configured
	 */
	protected String getDefaultHost() {
		return null;
	}

	/**
	 * Whether signing in needs a password in the SMSS. False for an engine that
	 * obtains its own credential, such as an access token.
	 *
	 * @return true when the password key is required
	 */
	protected boolean requiresPassword() {
		return true;
	}

	/**
	 * Add the jakarta.mail properties that describe how this engine signs in.
	 * Called while the session is being built, before the raw {@code mail.} keys
	 * from the SMSS, so a caller can still override any of them.
	 *
	 * @param mailProps the session properties being built
	 */
	protected void addAuthenticationProperties(Properties mailProps) {
		// password authentication needs nothing beyond the credentials themselves
	}

	/**
	 * What this engine signs in with, for the log line that records the connection.
	 *
	 * @return a short description of the credential
	 */
	protected String getCredentialDescription() {
		return "a password";
	}

	/**
	 * The user to sign in as.
	 *
	 * @return the username
	 */
	protected String getConnectUsername() {
		return this.username;
	}

	/**
	 * The secret to sign in with. Read on every connect rather than held, so an
	 * engine whose credential expires can hand over a current one.
	 *
	 * @return the password or token
	 */
	protected String getConnectPassword() {
		return this.password;
	}

	/**
	 * Throw away whatever credential was just refused, so the next connect obtains
	 * a new one.
	 *
	 * @return true when there is a new credential worth retrying with
	 */
	protected boolean refreshCredentials() {
		// a password out of an SMSS does not change between attempts
		return false;
	}

	/**
	 * The jakarta.mail property name for one setting of the protocol this engine
	 * connects over, so a subclass adding properties does not have to know whether
	 * the connection ended up on the plain or the encrypted protocol name.
	 *
	 * @param suffix the property, such as {@code auth.mechanisms}
	 * @return the property name
	 */
	protected String sessionProperty(String suffix) {
		return rawProperty(this.storeProtocol, suffix);
	}

	/**
	 * Fill in the function metadata that the SMSS did not define. The parameters
	 * describe what {@link #execute(Map)} understands, and their descriptions carry
	 * the limits this engine was opened with so a caller knows what it gets. A
	 * value set in the SMSS is never overwritten.
	 */
	protected void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			this.functionDescription = getDefaultFunctionDescription();
		}

		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaultParameters = new ArrayList<>();
			addProtocolParameters(defaultParameters);
			defaultParameters.add(new FunctionParameter(LIMIT_PARAM, "integer", """
					Optional. How many of the most recent matching messages to return. Defaults to %s and \
					cannot be more than %s.\
					""".formatted(this.defaultMessages, this.maxMessages)));
			defaultParameters.add(new FunctionParameter(FROM_PARAM, "string", """
					Optional. Only return messages whose sender contains this text.\
					""" + allowedSenderText()));
			defaultParameters.add(new FunctionParameter(SUBJECT_PARAM, "string", """
					Optional. Only return messages whose subject contains this text.\
					"""));
			defaultParameters.add(new FunctionParameter(SINCE_DAYS_PARAM, "integer", """
					Optional. Only return messages from the last this many days.\
					"""));
			defaultParameters.add(new FunctionParameter(INCLUDE_BODY_PARAM, "boolean", """
					Optional. Set to false to return only the headers of each message rather than what it \
					says. Defaults to true, and a body longer than %s characters comes back truncated.\
					""".formatted(this.maxBodyChars)));
			if (this.allowAttachmentDownload) {
				defaultParameters.add(new FunctionParameter(DOWNLOAD_ATTACHMENTS_PARAM, "boolean", """
						Optional. Set to true to save the attachments of every returned message into the files \
						of the insight making this call, so that they can then be opened. Defaults to false, \
						which lists the attachment names without downloading them.\
						"""));
			}
			this.parameters = defaultParameters;
		}
	}

	/**
	 * Add the parameters only one protocol understands, such as the folder to read.
	 * Called while the default function metadata is being built, before the
	 * parameters every mailbox shares.
	 *
	 * @param parameters the parameters being built
	 */
	protected void addProtocolParameters(List<FunctionParameter> parameters) {
		// nothing by default
	}

	/**
	 * What this engine tells a model it does, when the SMSS did not say.
	 *
	 * @return the function description
	 */
	protected abstract String getDefaultFunctionDescription();

	/**
	 * Build the sentence that tells a caller whose mail this engine will surface,
	 * so a model does not waste a call searching for a sender it cannot see.
	 *
	 * @return the sentence to append, or an empty string when there is no limit
	 */
	private String allowedSenderText() {
		if (this.allowedSenderDomains.isEmpty()) {
			return "";
		}
		return " Only messages from these domains are available: " + String.join(", ", this.allowedSenderDomains) + ".";
	}

	/**
	 * The jakarta.mail protocol name for a plaintext connection, such as
	 * {@code pop3}.
	 *
	 * @return the protocol name
	 */
	protected abstract String getProtocol();

	/**
	 * The jakarta.mail protocol name for a connection that is encrypted from the
	 * first byte, such as {@code pop3s}.
	 *
	 * @return the protocol name
	 */
	protected abstract String getSecureProtocol();

	/**
	 * The port this protocol is served on when the SMSS does not name one.
	 *
	 * @param secure whether the connection is encrypted from the first byte
	 * @return the default port
	 */
	protected abstract String getDefaultPort(boolean secure);

	/**
	 * Whether the store is opened on the protocol that is encrypted from the first
	 * byte rather than one that starts in the clear.
	 *
	 * @return true when the connection needs no upgrade
	 */
	protected boolean isSecureProtocol() {
		return getSecureProtocol().equalsIgnoreCase(this.storeProtocol);
	}

	/**
	 * The SMSS key this engine reads one of its connection settings from, so POP3
	 * and IMAP engines each get their own names for the same setting.
	 *
	 * @param suffix the setting, such as {@link #HOST_SUFFIX}
	 * @return the key name
	 */
	protected String key(String suffix) {
		return getProtocol().toUpperCase() + "_" + suffix;
	}

	/**
	 * Build a jakarta.mail property name for one protocol.
	 *
	 * @param protocol the protocol the property belongs to
	 * @param suffix   the property, such as {@code host}
	 * @return the property name
	 */
	private static String rawProperty(String protocol, String suffix) {
		return RAW_MAIL_PROPERTY_PREFIX + protocol + "." + suffix;
	}

	/**
	 * Put every raw jakarta.mail key back in the form the mail library answers to.
	 *
	 * <p>
	 * jakarta.mail property names are lower case and it ignores anything else,
	 * while the reactor that writes an SMSS upper cases every key it is given.
	 * Without this, a mail server configured through the UI with a property this
	 * engine does not model - the whole point of the {@code mail.} passthrough -
	 * would arrive as {@code MAIL.IMAP.SSL.TRUST} and be silently ignored.
	 *
	 * @param smssProp the engine properties as they were written
	 * @return the same properties with the raw mail keys lower cased
	 */
	private static Properties normalizeRawMailKeys(Properties smssProp) {
		Properties normalized = new Properties();
		for (String key : smssProp.stringPropertyNames()) {
			if (key.toLowerCase().startsWith(RAW_MAIL_PROPERTY_PREFIX)) {
				normalized.setProperty(key.toLowerCase(), smssProp.getProperty(key));
			} else {
				normalized.setProperty(key, smssProp.getProperty(key));
			}
		}
		return normalized;
	}

	/**
	 * Fill in what {@link AbstractFunctionEngine} needs for an engine that is built
	 * in memory rather than read out of a catalogued SMSS.
	 *
	 * @param engineId           the id to open under, used only for logging
	 * @param props              the mail server properties
	 * @param defaultDescription the function description to use when there is none
	 * @return the properties to open the engine with
	 */
	protected static Properties transientProperties(String engineId, Properties props, String defaultDescription) {
		Properties engineProps = new Properties();
		engineProps.putAll(props);
		engineProps.put(Constants.ENGINE, engineId);

		// the function metadata only matters to a model picking this out of a tool
		// list, which a transient engine is never in, but AbstractFunctionEngine
		// requires both to be present
		if (trimToNull(engineProps.getProperty(NAME_KEY)) == null) {
			engineProps.put(NAME_KEY, engineId);
		}
		if (trimToNull(engineProps.getProperty(DESCRIPTION_KEY)) == null) {
			engineProps.put(DESCRIPTION_KEY, defaultDescription);
		}
		return engineProps;
	}

	/**
	 * Put a value on the output only when there is one, so a caller reading the
	 * result is not handed a pile of nulls for the headers a message did not carry.
	 *
	 * @param output the map being built
	 * @param key    the key to set
	 * @param value  the value, possibly null
	 */
	protected static void putIfPresent(Map<String, Object> output, String key, Object value) {
		if (value != null) {
			output.put(key, value);
		}
	}

	/**
	 * Read one header of a message.
	 *
	 * @param message    the message
	 * @param headerName the header to read
	 * @return the first value of the header, or null when it is not there
	 * @throws MessagingException when the message cannot be read
	 */
	protected static String firstHeader(Message message, String headerName) throws MessagingException {
		String[] values = message.getHeader(headerName);
		if (values == null || values.length == 0) {
			return null;
		}
		return trimToNull(values[0]);
	}

	/**
	 * Render the addresses of one header the way they would be typed.
	 *
	 * @param addresses the addresses, possibly null
	 * @return the addresses joined, or null when there are none
	 */
	protected static String joinAddresses(Address[] addresses) {
		if (addresses == null || addresses.length == 0) {
			return null;
		}
		List<String> values = new ArrayList<>();
		for (Address address : addresses) {
			if (address != null) {
				values.add(address.toString());
			}
		}
		if (values.isEmpty()) {
			return null;
		}
		return String.join(", ", values);
	}

	/**
	 * Render a mail date as an instant, so a caller is not left parsing whatever
	 * format the sending mail client happened to use.
	 *
	 * @param date the date, possibly null
	 * @return the date as an ISO instant, or null when there is none
	 */
	protected static String formatDate(Date date) {
		if (date == null) {
			return null;
		}
		return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(date.getTime()));
	}

	/**
	 * Pull one runtime parameter as a list, accepting both the comma separated
	 * string a model sends and the list a java caller already holds.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @param key             the parameter to read
	 * @return the entries, empty when the parameter was not passed
	 */
	protected static List<String> parameterAsList(Map<String, Object> parameterValues, String key) {
		Object value = parameterValues == null ? null : parameterValues.get(key);
		if (value == null) {
			return new ArrayList<>();
		}
		if (value instanceof Object[]) {
			value = Arrays.asList((Object[]) value);
		}
		if (value instanceof Collection) {
			List<String> entries = new ArrayList<>();
			for (Object entry : (Collection<?>) value) {
				if (entry == null) {
					continue;
				}
				String stringEntry = entry.toString().trim();
				if (!stringEntry.isEmpty()) {
					entries.add(stringEntry);
				}
			}
			return entries;
		}
		return splitList(value.toString());
	}

	/**
	 * Split a comma or semicolon separated value into its entries, dropping blanks.
	 *
	 * @param value the raw value, possibly null
	 * @return the entries, empty when there are none
	 */
	protected static List<String> splitList(String value) {
		List<String> entries = new ArrayList<>();
		if (value == null) {
			return entries;
		}
		for (String entry : value.split("[,;]")) {
			entry = entry.trim();
			if (!entry.isEmpty()) {
				entries.add(entry);
			}
		}
		return entries;
	}

	/**
	 * Read an SMSS flag, treating an unset or blank key as the default rather than
	 * as false, since the UI writes a blank line for every field left empty.
	 *
	 * @param value        the raw SMSS value
	 * @param defaultValue value to use when the key is not set
	 * @return the flag
	 */
	protected static boolean parseBoolean(String value, boolean defaultValue) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return defaultValue;
		}
		return Boolean.parseBoolean(value);
	}

	/**
	 * Trim a raw SMSS value, treating blank as unset.
	 *
	 * @param value the raw value, possibly null
	 * @return the trimmed value, or null when there is nothing there
	 */
	protected static String trimToNull(String value) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return null;
		}
		return value;
	}

	/**
	 * The first value that is actually set, used where the same setting can be
	 * spelled several ways in an SMSS.
	 *
	 * @param values the values in the order they are preferred
	 * @return the first non null value, or null when there is none
	 */
	protected static String firstNonNull(String... values) {
		for (String value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	private void closeStore() {
		try {
			this.mailStore.close();
		} catch (MessagingException e) {
			classLogger.warn("Error closing the connection to " + this.host, e);
		} finally {
			this.mailStore = null;
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (this.mailStore != null) {
			closeStore();
		}
	}

	/**
	 * What a caller asked to match on for one search. A field left null was not
	 * asked about at all, which is not the same as being asked to match nothing.
	 */
	protected static class MailSearchCriteria {

		protected String from = null;
		protected String subject = null;
		protected Date since = null;
		protected boolean unreadOnly = false;

		/**
		 * Whether this search asks for anything, or is just the last messages in the
		 * folder.
		 *
		 * @return true when nothing was asked about
		 */
		protected boolean isEmpty() {
			return this.from == null && this.subject == null && this.since == null && !this.unreadOnly;
		}

	}

}
