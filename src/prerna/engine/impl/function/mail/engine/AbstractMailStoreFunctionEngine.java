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

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.function.AbstractFunctionEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.function.mail.adapter.graph.GraphMailboxClient;
import prerna.engine.impl.function.mail.adapter.jakarta.auth.MailStoreAuthentication;
import prerna.engine.impl.function.mail.adapter.jakarta.auth.PasswordStoreAuthentication;
import prerna.engine.impl.function.mail.attachment.AttachmentStore;
import prerna.engine.impl.function.mail.auth.microsoft365.Microsoft365MailOAuth;
import prerna.engine.impl.function.mail.config.JakartaStoreConfig;
import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.config.Microsoft365Config;
import prerna.engine.impl.function.mail.model.MailSearchCriteria;
import prerna.engine.impl.function.mail.model.MailSearchRequest;
import prerna.engine.impl.function.mail.policy.MailReadPolicy;
import prerna.engine.impl.function.mail.spi.MailboxClient;
import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.om.Insight;
import prerna.util.Constants;

/**
 * What a reading mail engine looks like from outside.
 *
 * <p>
 * This is the only part that knows it is a function engine. It publishes the
 * parameters a caller sees, turns the map of values they passed into a
 * {@link MailSearchRequest}, and answers with a map. Reaching the mailbox is a
 * {@link MailboxClient}, and how much may be returned is a
 * {@link MailReadPolicy}, neither of which knows anything about engines.
 *
 * <p>
 * Which client is built is decided once, when the engine opens, from
 * {@code MAIL_TRANSPORT}. After that nothing asks again: a search runs the same
 * way whether the mailbox is reached over a protocol or over Graph, and the two
 * answer in the same shape.
 *
 * <p>
 * A subclass supplies its protocol's names and defaults, and may add parameters
 * and actions of its own. IMAP is the one with more to offer, since it is the
 * only protocol here that can change a mailbox.
 */
public abstract class AbstractMailStoreFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractMailStoreFunctionEngine.class);

	protected static final String HOST_SUFFIX = "HOST";
	protected static final String PORT_SUFFIX = "PORT";
	protected static final String USERNAME_SUFFIX = "USERNAME";
	protected static final String PASSWORD_SUFFIX = "PASSWORD";
	protected static final String SECURITY_SUFFIX = "SECURITY";

	public static final String ONLY_CUSTOM_PROPS_KEY = MailProperties.ONLY_CUSTOM_PROPERTIES;
	public static final String CONNECTION_TIMEOUT_KEY = MailProperties.CONNECTION_TIMEOUT;
	public static final String READ_TIMEOUT_KEY = MailProperties.READ_TIMEOUT;
	public static final String MAX_MESSAGES_KEY = MailProperties.MAX_MESSAGES;
	public static final String DEFAULT_MESSAGES_KEY = MailProperties.DEFAULT_MESSAGES;
	public static final String MAX_BODY_CHARS_KEY = MailProperties.MAX_BODY_CHARS;
	public static final String ALLOWED_SENDER_DOMAINS_KEY = MailProperties.ALLOWED_SENDER_DOMAINS;
	public static final String ALLOW_ATTACHMENT_DOWNLOAD_KEY = MailProperties.ALLOW_ATTACHMENT_DOWNLOAD;
	public static final String MAX_ATTACHMENT_SIZE_KEY = MailProperties.MAX_ATTACHMENT_SIZE;

	public static final String RAW_MAIL_PROPERTY_PREFIX = MailProperties.RAW_MAIL_PROPERTY_PREFIX;
	public static final String STORE_PROTOCOL_PROPERTY = MailProperties.STORE_PROTOCOL;

	public static final String SSL_SECURITY = MailProperties.SSL_SECURITY;
	public static final String STARTTLS_SECURITY = MailProperties.STARTTLS_SECURITY;
	public static final String NONE_SECURITY = MailProperties.NO_SECURITY;
	public static final String SEARCH_ACTION = "search";

	protected static final String ACTION_PARAM = "action";
	protected static final String FOLDER_PARAM = "folder";
	protected static final String LIMIT_PARAM = "limit";
	protected static final String FROM_PARAM = "from";
	protected static final String SUBJECT_PARAM = "subject";
	protected static final String SINCE_DAYS_PARAM = "sinceDays";
	protected static final String UNREAD_ONLY_PARAM = "unreadOnly";
	protected static final String INCLUDE_BODY_PARAM = "includeBody";
	protected static final String DOWNLOAD_ATTACHMENTS_PARAM = "downloadAttachments";
	protected static final String INBOX_FOLDER = "INBOX";

	// Retained for protocol-specific metadata and diagnostics.
	protected String host;
	protected String port;
	protected String username;
	protected String password;
	protected String security = SSL_SECURITY;
	protected String storeProtocol;
	protected int maxMessages;
	protected int defaultMessages;
	protected int maxBodyChars;
	protected long maxAttachmentSize;
	protected boolean allowAttachmentDownload;
	protected Set<String> allowedSenderDomains = Set.of();
	protected String transportName;

	private MailReadPolicy readPolicy;
	private AttachmentStore attachmentStore;
	private MailboxClient mailboxClient;

	@Override
	public void open(Properties sourceProperties) throws Exception {
		super.open(sourceProperties);
		Properties properties = MailProperties.normalize(sourceProperties);
		this.readPolicy = MailReadPolicy.from(properties);
		this.attachmentStore = new AttachmentStore(this.readPolicy.maxAttachmentSize());
		this.maxMessages = this.readPolicy.maxMessages();
		this.defaultMessages = this.readPolicy.defaultMessages();
		this.maxBodyChars = this.readPolicy.maxBodyChars();
		this.maxAttachmentSize = this.readPolicy.maxAttachmentSize();
		this.allowAttachmentDownload = this.readPolicy.allowAttachmentDownload();
		this.allowedSenderDomains = this.readPolicy.allowedSenderDomains();

		this.transportName = Microsoft365MailOAuth.resolveTransport(properties, getDefaultTransport());
		this.username = MailProperties.trimToNull(properties.getProperty(key(USERNAME_SUFFIX)));
		if (this.username == null) {
			throw new IllegalArgumentException(
					"Must define " + key(USERNAME_SUFFIX) + " to know which mailbox to open");
		}
		this.password = MailProperties.trimToNull(properties.getProperty(key(PASSWORD_SUFFIX)));

		openProtocolProperties(properties);
		if (isGraphTransport()) {
			Microsoft365Config microsoft = Microsoft365Config.from(properties, Microsoft365MailOAuth.GRAPH_SCOPE);
			MicrosoftGraphAppTokenProvider tokenProvider = microsoft.tokenProvider();
			this.mailboxClient = new GraphMailboxClient(new MicrosoftOutlookMailHelper(microsoft.graphBaseUrl()),
					tokenProvider, this.username, this.readPolicy, this.attachmentStore, this::getAuthenticationHint,
					markGraphSearchAsRead());
			classLogger.info("Reading {} through Microsoft Graph, signing in with {}", this.username,
					Microsoft365MailOAuth.credentialDescription(tokenProvider.getClientId()));
		} else {
			JakartaStoreConfig config = JakartaStoreConfig.from(properties, getProtocol(), getSecureProtocol(),
					getDefaultHost(), getDefaultPort(false), getDefaultPort(true), requiresPassword());
			this.host = config.host();
			this.port = Integer.toString(config.port());
			this.username = config.username();
			this.password = config.password();
			this.security = config.security();
			this.storeProtocol = config.storeProtocol();
			this.mailboxClient = createJakartaClient(config, createStoreAuthentication(properties, config),
					this.readPolicy, this.attachmentStore);
		}
		setDefaultFunctionMetadata();
	}

	/**
	 * Read whatever this engine needs beyond what every mail engine takes.
	 *
	 * <p>
	 * Called before a client is built, so a subclass that validates credentials
	 * here fails while the engine is being cataloged rather than on the first
	 * search.
	 *
	 * @param properties the engine's SMSS properties
	 */
	protected void openProtocolProperties(Properties properties) {
		// a plain mailbox needs nothing beyond the host, mailbox and password
	}

	/**
	 * How this engine signs in over a protocol.
	 *
	 * @param properties the engine's SMSS properties
	 * @param config     the connection settings already read out of them
	 * @return the way to sign in, which is a password unless a subclass says
	 *         otherwise
	 */
	protected MailStoreAuthentication createStoreAuthentication(Properties properties, JakartaStoreConfig config) {
		return new PasswordStoreAuthentication(config.password());
	}

	/**
	 * Build the client for this engine's protocol.
	 *
	 * @param config          which mail server to reach and how
	 * @param authentication  how to sign in to it
	 * @param policy          how much of the mailbox may be returned
	 * @param attachmentStore where an attachment is written when one is asked for
	 * @return the client
	 */
	protected abstract MailboxClient createJakartaClient(JakartaStoreConfig config,
			MailStoreAuthentication authentication, MailReadPolicy policy, AttachmentStore attachmentStore);

	/**
	 * @return whether reading a message over Graph marks it read, which only an
	 *         engine whose protocol has read state should say yes to
	 */
	protected boolean markGraphSearchAsRead() {
		return false;
	}

	/**
	 * Search the mailbox, or do something else to it.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @return what was found, or what the action did
	 * @throws IllegalArgumentException when the call asks for something this engine
	 *                                  does not permit, or the mailbox cannot be
	 *                                  reached
	 */
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		Map<String, Object> parameters = parameterValues == null ? new HashMap<>() : new HashMap<>(parameterValues);
		Insight insight = (Insight) parameters.remove(Constants.INSIGHT);
		validateRequiredParameters(parameters);
		String action = getParameterValue(parameters, ACTION_PARAM, SEARCH_ACTION);
		return SEARCH_ACTION.equalsIgnoreCase(action) ? searchMessages(parameters, insight)
				: executeMailAction(action, parameters);
	}

	/**
	 * Do something to the mailbox other than search it.
	 *
	 * <p>
	 * Refused by default, since most of these engines only read. A subclass whose
	 * protocol can change a mailbox overrides this, and is still expected to check
	 * that its SMSS turned the change on.
	 *
	 * @param action          what was asked for
	 * @param parameterValues the runtime parameters for this call
	 * @return what the action did
	 * @throws IllegalArgumentException when this engine cannot do it
	 */
	protected Object executeMailAction(String action, Map<String, Object> parameterValues) {
		throw new IllegalArgumentException(
				"The '" + action + "' action is not available. The only action available is " + SEARCH_ACTION);
	}

	/**
	 * Turn one call's parameters into a search and run it.
	 *
	 * <p>
	 * A limit beyond what the engine returns is held down rather than refused, and
	 * asking to save attachments when the SMSS has not allowed it is ignored rather
	 * than refused. Both are logged. Neither is worth failing a read over, which is
	 * the opposite of how sending treats the same kind of overreach - a read that
	 * returns less than was asked for is recoverable, where a message sent to the
	 * wrong place is not.
	 *
	 * @param parameters the runtime parameters for this call
	 * @param insight    the insight this call is running under, or null
	 * @return what was found
	 */
	protected Map<String, Object> searchMessages(Map<String, Object> parameters, Insight insight) {
		String folder = resolveFolderName(getParameterValue(parameters, FOLDER_PARAM, null));
		int requestedLimit = getIntParameterValue(parameters, LIMIT_PARAM, this.defaultMessages);
		int limit = this.readPolicy.boundedLimit(requestedLimit);
		if (limit != requestedLimit) {
			classLogger.warn("A limit of {} was bounded to {}", requestedLimit, limit);
		}

		int sinceDays = getIntParameterValue(parameters, SINCE_DAYS_PARAM, -1);
		Instant since = sinceDays > 0 ? Instant.now().minus(sinceDays, ChronoUnit.DAYS) : null;
		MailSearchCriteria criteria = new MailSearchCriteria(getParameterValue(parameters, FROM_PARAM, null),
				getParameterValue(parameters, SUBJECT_PARAM, null), since,
				getBooleanParameterValue(parameters, UNREAD_ONLY_PARAM, false));
		boolean includeBody = getBooleanParameterValue(parameters, INCLUDE_BODY_PARAM, true);
		boolean downloadAttachments = getBooleanParameterValue(parameters, DOWNLOAD_ATTACHMENTS_PARAM, false);
		if (downloadAttachments && !this.readPolicy.allowAttachmentDownload()) {
			classLogger.warn("Attachment download was requested but {} is not enabled", ALLOW_ATTACHMENT_DOWNLOAD_KEY);
			downloadAttachments = false;
		}
		return this.mailboxClient
				.search(new MailSearchRequest(folder, criteria, limit, includeBody, downloadAttachments, insight))
				.toMap();
	}

	/**
	 * @return the mailbox this engine reads, for a subclass with actions of its own
	 */
	protected MailboxClient mailboxClient() {
		return this.mailboxClient;
	}

	/**
	 * Work out which folder to read.
	 *
	 * <p>
	 * Answers with the inbox whatever was asked for, because most of these
	 * protocols have only one folder. A named folder is warned about rather than
	 * refused, since returning the inbox is what the caller was going to get from
	 * this protocol anyway. IMAP overrides this.
	 *
	 * @param requestedFolder the folder the call asked for, or null
	 * @return the folder to read
	 */
	protected String resolveFolderName(String requestedFolder) {
		if (requestedFolder != null && !requestedFolder.equalsIgnoreCase(INBOX_FOLDER)) {
			classLogger.warn("{} only has an {} folder, reading it rather than {}", getProtocol(), INBOX_FOLDER,
					requestedFolder);
		}
		return INBOX_FOLDER;
	}

	/**
	 * @param sender the address a message came from
	 * @return whether this engine will return it
	 */
	protected boolean isSenderAllowed(String sender) {
		return this.readPolicy.isSenderAllowed(sender);
	}

	/**
	 * @return what to check when the mailbox refuses the sign in, or null when
	 *         there is nothing useful to add, which is the case for a plain mailbox
	 *         whose password is simply wrong
	 */
	protected String getAuthenticationHint() {
		return null;
	}

	/**
	 * @return how this engine reads when the SMSS does not say, which is the
	 *         protocol for a plain mailbox and Graph for a Microsoft 365 one
	 */
	protected String getDefaultTransport() {
		return Microsoft365MailOAuth.JAKARTA_TRANSPORT;
	}

	/**
	 * @return whether this engine reads through Graph rather than a protocol
	 */
	protected boolean isGraphTransport() {
		return Microsoft365MailOAuth.GRAPH_TRANSPORT.equals(this.transportName);
	}

	/**
	 * @return the mail server to assume when the SMSS names none, which only an
	 *         engine for a known service has, or null to insist on one
	 */
	protected String getDefaultHost() {
		return null;
	}

	/**
	 * @return whether a missing password is an error, which it is not for an engine
	 *         that signs in with a token
	 */
	protected boolean requiresPassword() {
		return true;
	}

	/**
	 * Describe this engine the way it is actually configured, unless whoever
	 * cataloged it wrote their own description and parameters.
	 *
	 * <p>
	 * The limits go into the parameter descriptions rather than being enforced
	 * silently, so a caller reading them knows what it can ask for. A parameter for
	 * something the SMSS has not enabled is not published at all.
	 */
	protected void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			this.functionDescription = getDefaultFunctionDescription();
		}
		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaults = new ArrayList<>();
			addProtocolParameters(defaults);
			defaults.add(new FunctionParameter(LIMIT_PARAM, "integer",
					"Optional. Number of recent matches to return. Defaults to " + this.defaultMessages
							+ " and cannot exceed " + this.maxMessages + "."));
			defaults.add(new FunctionParameter(FROM_PARAM, "string",
					"Optional. Only return messages whose sender contains this text." + allowedSenderText()));
			defaults.add(new FunctionParameter(SUBJECT_PARAM, "string",
					"Optional. Only return messages whose subject contains this text."));
			defaults.add(new FunctionParameter(SINCE_DAYS_PARAM, "integer",
					"Optional. Only return messages from the last this many days."));
			defaults.add(new FunctionParameter(INCLUDE_BODY_PARAM, "boolean",
					"Optional. Set false to return headers only. Defaults to true; bodies over " + this.maxBodyChars
							+ " characters are truncated."));
			if (this.allowAttachmentDownload) {
				defaults.add(new FunctionParameter(DOWNLOAD_ATTACHMENTS_PARAM, "boolean",
						"Optional. Set true to save attachments into the calling insight. Defaults to false."));
			}
			this.parameters = defaults;
		}
	}

	/**
	 * Add the parameters that only this engine's protocol offers, which go first
	 * because they are the ones a caller is most likely to need.
	 *
	 * @param parameters the parameter list being built
	 */
	protected void addProtocolParameters(List<FunctionParameter> parameters) {
		// a single folder mailbox with no actions has nothing of its own to offer
	}

	/**
	 * @return a sentence naming the domains this engine will return mail from, or
	 *         empty when any sender is allowed
	 */
	private String allowedSenderText() {
		return this.allowedSenderDomains.isEmpty() ? ""
				: " Only messages from these domains are available: " + String.join(", ", this.allowedSenderDomains)
						+ ".";
	}

	/**
	 * @return how to describe this function when the SMSS does not, which is what a
	 *         model calling it reads to decide whether it is the right one
	 */
	protected abstract String getDefaultFunctionDescription();

	/**
	 * @return the plain protocol name, which also decides how this engine's SMSS
	 *         keys are spelled
	 */
	protected abstract String getProtocol();

	/**
	 * @return the implicit TLS name of that protocol
	 */
	protected abstract String getSecureProtocol();

	/**
	 * @param secure whether the connection speaks TLS from the first byte
	 * @return the port to use when the SMSS names none
	 */
	protected abstract String getDefaultPort(boolean secure);

	/**
	 * @param suffix the setting, such as {@code HOST}
	 * @return this engine's SMSS key for it, such as {@code IMAP_HOST}
	 */
	protected String key(String suffix) {
		return getProtocol().toUpperCase() + "_" + suffix;
	}

	/**
	 * Fill in the name and description an engine that is not in the catalog has no
	 * one to give it.
	 *
	 * @param engineId    the id to open under
	 * @param properties  the settings the caller supplied
	 * @param description what to describe the function as
	 * @return the settings, ready to open with
	 */
	protected static Properties transientProperties(String engineId, Properties properties, String description) {
		return MailProperties.transientProperties(engineId, properties, description, NAME_KEY, DESCRIPTION_KEY);
	}

	/**
	 * @param values the runtime parameters for this call
	 * @param key    the parameter to read
	 * @return its entries, whether it arrived as a list, an array or a separated
	 *         string
	 */
	protected static List<String> parameterAsList(Map<String, Object> values, String key) {
		return MailProperties.parameterAsList(values, key);
	}

	/**
	 * @param value a comma or semicolon separated list, which may be null
	 * @return the entries, empty when there are none
	 */
	protected static List<String> splitList(String value) {
		return MailProperties.splitList(value);
	}

	/**
	 * @param value        the configured value
	 * @param defaultValue what it is when nothing was configured
	 * @return the value
	 */
	protected static boolean parseBoolean(String value, boolean defaultValue) {
		return MailProperties.parseBoolean(value, defaultValue);
	}

	/**
	 * @param value the value to trim
	 * @return it without surrounding space, or null when there is nothing left
	 */
	protected static String trimToNull(String value) {
		return MailProperties.trimToNull(value);
	}

	/**
	 * Release the connection to the mailbox, if this engine's way of reading holds
	 * one.
	 */
	@Override
	public void close() throws IOException {
		if (this.mailboxClient != null) {
			this.mailboxClient.close();
		}
	}
}
