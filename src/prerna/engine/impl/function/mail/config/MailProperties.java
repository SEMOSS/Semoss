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
package prerna.engine.impl.function.mail.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import prerna.util.Constants;

/**
 * What every mail engine calls its settings, and how it reads them.
 *
 * <p>
 * The names live in one place because most of them are shared: an engine that
 * sends and an engine that reads both take timeouts and both take an app
 * registration, and a key spelled two ways in two engines is a key somebody
 * will set on the wrong one. The parsing is here for the same reason - a list
 * is split, a boolean is defaulted and a blank is treated as absent identically
 * wherever it appears.
 *
 * <p>
 * Settings arrive as an SMSS file, whose keys are upper cased by the reactor
 * that creates an engine. That is why the engine's own keys are upper case and
 * why {@link #normalize(Properties)} exists: the raw jakarta.mail keys are case
 * sensitive and have to be put back.
 */
public final class MailProperties {

	/** What marks a setting as one to hand to jakarta.mail untouched. */
	public static final String RAW_MAIL_PROPERTY_PREFIX = "mail.";

	/** Which way an engine reaches a Microsoft 365 mailbox. */
	public static final String MAIL_TRANSPORT = "MAIL_TRANSPORT";

	/** Through the Microsoft Graph API. */
	public static final String GRAPH_TRANSPORT = "graph";

	/** Through the mail protocol itself. */
	public static final String JAKARTA_TRANSPORT = "jakarta";

	// the app registration, which is the same five keys for every Microsoft 365
	// engine and for both transports
	public static final String EXCHANGE_TENANT = "EXCHANGE_TENANT";
	public static final String EXCHANGE_CLIENT_ID = "EXCHANGE_CLIENT_ID";
	public static final String EXCHANGE_CLIENT_SECRET = "EXCHANGE_CLIENT_SECRET";
	public static final String EXCHANGE_SCOPE = "EXCHANGE_SCOPE";
	public static final String GRAPH_BASE_URL = "GRAPH_BASE_URL";
	public static final String SAVE_TO_SENT_ITEMS = "SAVE_TO_SENT_ITEMS";

	// how a connection behaves, and how much of a mailbox a read may return
	public static final String ONLY_CUSTOM_PROPERTIES = "ONLY_CUSTOM_PROPS";
	public static final String CONNECTION_TIMEOUT = "CONNECTION_TIMEOUT";
	public static final String READ_TIMEOUT = "READ_TIMEOUT";
	public static final String MAX_MESSAGES = "MAX_MESSAGES";
	public static final String DEFAULT_MESSAGES = "DEFAULT_MESSAGES";
	public static final String MAX_BODY_CHARS = "MAX_BODY_CHARS";
	public static final String ALLOWED_SENDER_DOMAINS = "ALLOWED_SENDER_DOMAINS";
	public static final String ALLOW_ATTACHMENT_DOWNLOAD = "ALLOW_ATTACHMENT_DOWNLOAD";
	public static final String MAX_ATTACHMENT_SIZE = "MAX_ATTACHMENT_SIZE";

	// what a send is allowed to do, which is where the send policy is set
	public static final String SMTP_SENDER = "SMTP_SENDER";
	public static final String SMTP_SENDER_NAME = "SMTP_SENDER_NAME";
	public static final String ALLOW_SENDER_OVERRIDE = "ALLOW_SENDER_OVERRIDE";
	public static final String ALLOWED_RECIPIENT_DOMAINS = "ALLOWED_RECIPIENT_DOMAINS";
	public static final String DEFAULT_TO = "DEFAULT_TO";
	public static final String DEFAULT_CC = "DEFAULT_CC";
	public static final String DEFAULT_BCC = "DEFAULT_BCC";
	public static final String SUBJECT_PREFIX = "SUBJECT_PREFIX";
	public static final String HTML = "HTML";
	public static final String MAX_RECIPIENTS = "MAX_RECIPIENTS";
	public static final String ALLOW_ATTACHMENTS = "ALLOW_ATTACHMENTS";

	// which relay to send through. the reading engines spell these with their own
	// protocol in front, since an engine reads one mailbox and sends through none
	public static final String SMTP_HOST = "SMTP_HOST";
	public static final String SMTP_PORT = "SMTP_PORT";
	public static final String SMTP_USERNAME = "SMTP_USERNAME";
	public static final String SMTP_PASSWORD = "SMTP_PASSWORD";
	public static final String SMTP_SECURITY = "SMTP_SECURITY";

	/**
	 * Names the protocol outright, overriding what the security setting implies.
	 */
	public static final String STORE_PROTOCOL = "mail.store.protocol";

	/** TLS from the first byte, on the protocol's own encrypted port. */
	public static final String SSL_SECURITY = "ssl";

	/** Start plain and upgrade, which is refused if the server will not. */
	public static final String STARTTLS_SECURITY = "starttls";

	/** No encryption at all, which is for a test server and nothing else. */
	public static final String NO_SECURITY = "none";

	/**
	 * Which implementation is underneath, once {@link #MAIL_TRANSPORT} has been
	 * read.
	 */
	public enum Backend {
		GRAPH, JAKARTA
	}

	private MailProperties() {

	}

	/**
	 * Work out which way this engine reaches its mailbox.
	 *
	 * <p>
	 * An unrecognized value is refused rather than quietly treated as the default,
	 * since a typo would otherwise send mail through something the person setting
	 * it up did not choose.
	 *
	 * @param properties     the engine's SMSS properties
	 * @param defaultBackend what to use when the SMSS does not say, which differs
	 *                       between a plain engine and an Exchange one
	 * @return the backend to use
	 * @throws IllegalArgumentException when the SMSS names something else
	 */
	public static Backend backend(Properties properties, Backend defaultBackend) {
		String configured = firstNonNull(trimToNull(properties.getProperty(MAIL_TRANSPORT)),
				defaultBackend.name().toLowerCase(Locale.ROOT));
		if (GRAPH_TRANSPORT.equalsIgnoreCase(configured)) {
			return Backend.GRAPH;
		}
		if (JAKARTA_TRANSPORT.equalsIgnoreCase(configured)) {
			return Backend.JAKARTA;
		}
		throw new IllegalArgumentException("The " + MAIL_TRANSPORT + " of '" + configured
				+ "' is not one a mail engine can use, which is " + GRAPH_TRANSPORT + " or " + JAKARTA_TRANSPORT);
	}

	/**
	 * Put the raw jakarta.mail keys back to lower case.
	 *
	 * <p>
	 * The reactor that catalogs an engine upper cases every key it is given, which
	 * is right for this codebase's own names and wrong for jakarta.mail's, since
	 * those are matched case sensitively and are simply ignored in any other
	 * spelling. Without this a {@code mail.imaps.ssl.trust} set in an SMSS would
	 * have no effect at all and nothing would say so.
	 *
	 * @param properties the properties as the SMSS produced them
	 * @return the same settings with the {@code mail.} ones spelled the way
	 *         jakarta.mail reads them
	 */
	public static Properties normalize(Properties properties) {
		Properties normalized = new Properties();
		for (String key : properties.stringPropertyNames()) {
			String normalizedKey = key.toLowerCase(Locale.ROOT).startsWith(RAW_MAIL_PROPERTY_PREFIX)
					? key.toLowerCase(Locale.ROOT)
					: key;
			normalized.setProperty(normalizedKey, properties.getProperty(key));
		}
		return normalized;
	}

	/**
	 * The jakarta.mail name for one setting of a protocol.
	 *
	 * @param protocol the protocol, such as {@code imaps}
	 * @param suffix   the setting, such as {@code host}
	 * @return the property name
	 */
	public static String rawProperty(String protocol, String suffix) {
		return RAW_MAIL_PROPERTY_PREFIX + protocol + "." + suffix;
	}

	/**
	 * @param value the value to trim
	 * @return the value without surrounding space, or null when there is nothing
	 *         left of it, so a key set to blank reads the same as one never set
	 */
	public static String trimToNull(String value) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return null;
		}
		return value;
	}

	/**
	 * @param value        the configured value
	 * @param defaultValue what it is when nothing was configured
	 * @return the value, defaulted rather than read as false when it is absent
	 */
	public static boolean parseBoolean(String value, boolean defaultValue) {
		value = trimToNull(value);
		return value == null ? defaultValue : Boolean.parseBoolean(value);
	}

	/**
	 * Read a setting that names several things.
	 *
	 * <p>
	 * Both separators are accepted because a list of addresses gets written each
	 * way and neither is wrong.
	 *
	 * @param value a comma or semicolon separated list, which may be null
	 * @return the entries, trimmed and with the blanks dropped, empty when there
	 *         are none
	 */
	public static List<String> splitList(String value) {
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
	 * Read one runtime parameter that names several things.
	 *
	 * <p>
	 * Unlike a setting out of an SMSS, this arrives from whatever called the
	 * function, so it can already be a list or an array as easily as a string. All
	 * three are accepted rather than making the caller know which one the engine
	 * wanted.
	 *
	 * @param parameters the parameters for this call, which may be null
	 * @param key        the parameter to read
	 * @return the entries, empty when the parameter was not passed
	 */
	public static List<String> parameterAsList(Map<String, Object> parameters, String key) {
		Object value = parameters == null ? null : parameters.get(key);
		if (value == null) {
			return new ArrayList<>();
		}
		if (value instanceof Object[] values) {
			value = List.of(values);
		}
		if (value instanceof Collection<?> values) {
			List<String> entries = new ArrayList<>();
			for (Object entry : values) {
				String stringEntry = entry == null ? null : trimToNull(entry.toString());
				if (stringEntry != null) {
					entries.add(stringEntry);
				}
			}
			return entries;
		}
		return splitList(value.toString());
	}

	/**
	 * @param values the candidates, in the order they take precedence
	 * @return the first that is set, or null when none are
	 */
	public static String firstNonNull(String... values) {
		for (String value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	/**
	 * Fill in what an engine that is not in the catalog has no one to give it.
	 *
	 * <p>
	 * A transient engine is opened for a single call - a one time send, or the
	 * instance wide mail server out of {@code social.properties} - so there is no
	 * catalog entry naming or describing it. It still publishes a function
	 * definition, so it needs both, and the engine id is the only thing on hand to
	 * build them from.
	 *
	 * @param engineId           the id to open under, used only for logging
	 * @param properties         the settings the caller supplied
	 * @param defaultDescription what to describe the function as
	 * @param nameKey            the property holding the function name
	 * @param descriptionKey     the property holding the function description
	 * @return the settings, with a name and description filled in where the caller
	 *         left them out
	 */
	public static Properties transientProperties(String engineId, Properties properties, String defaultDescription,
			String nameKey, String descriptionKey) {
		Properties engineProperties = new Properties();
		engineProperties.putAll(properties);
		engineProperties.put(Constants.ENGINE, engineId);
		if (trimToNull(engineProperties.getProperty(nameKey)) == null) {
			engineProperties.put(nameKey, engineId);
		}
		if (trimToNull(engineProperties.getProperty(descriptionKey)) == null) {
			engineProperties.put(descriptionKey, defaultDescription);
		}
		return engineProperties;
	}
}
