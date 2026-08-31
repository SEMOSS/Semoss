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
package prerna.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.engine.impl.function.mail.IMAPFunctionEngine;
import prerna.engine.impl.function.mail.POP3FunctionEngine;
import prerna.engine.impl.function.mail.SMTPFunctionEngine;

public final class SocialPropertiesProcessor {

	// the engine ids the instance wide mail connections are opened under. they are
	// never in the catalog, so these only show up in the logs
	private static final String SMTP_ENGINE_ID = "SOCIAL_PROPERTIES_SMTP";
	private static final String POP3_ENGINE_ID = "SOCIAL_PROPERTIES_POP3";
	private static final String IMAP_ENGINE_ID = "SOCIAL_PROPERTIES_IMAP";

	public static final String SMTP_ENABLED = "smtp_enabled";
	public static final String SMTP_ONLY_CUSTOM_PROPS = "smtp_only_custom_props";
	public static final String SMTP_USERNAME = "smtp_username";
	public static final String SMTP_PASSWORD = "smtp_password";
	public static final String SMTP_SENDER = "smtp_sender";

	public static final String POP3_ENABLED = "pop3_enabled";
	public static final String POP3_ONLY_CUSTOM_PROPS = "pop3_only_custom_props";
	public static final String POP3_USERNAME = "pop3_username";
	public static final String POP3_PASSWORD = "pop3_password";

	public static final String IMAP_ENABLED = "imap_enabled";
	public static final String IMAP_ONLY_CUSTOM_PROPS = "imap_only_custom_props";
	public static final String IMAP_USERNAME = "imap_username";
	public static final String IMAP_PASSWORD = "imap_password";

	private static final Logger classLogger = LogManager.getLogger(SocialPropertiesProcessor.class);

	private String socialPropFile = null;

	// social properties
	private Properties socialData = null;
	@Deprecated
	private Map<String, Boolean> loginsAllowedMap;
	private List<Map<String, Object>> availableProviders;

	// smtp. the connection is held as an engine rather than a bare session so the
	// jakarta.mail handling lives in one place
	private SMTPFunctionEngine smtpEngine = null;
	// pulling out email properties for performance
	private Properties smtpEmailProps = null;
	private Map<String, String> smtpEmailStaticProps = null;

	// pop3. also held as an engine, which reopens the connection after the mail
	// server has dropped an idle one
	private POP3FunctionEngine pop3Engine = null;
	// pulling out email properties for performance
	private Properties pop3EmailProps = null;

	// imap
	private IMAPFunctionEngine imapEngine = null;
	// pulling out email properties for performance
	private Properties imapEmailProps = null;

	/**
	 * Creates a processor backed by the provided social properties file and loads
	 * it into memory.
	 *
	 * @param socialPropFile path to the social properties file
	 * @throws NullPointerException     if {@code socialPropFile} is null
	 * @throws IllegalArgumentException if the file does not exist
	 */
	public SocialPropertiesProcessor(String socialPropFile) {
		this.socialPropFile = socialPropFile;
		if (this.socialPropFile == null) {
			throw new NullPointerException("Must pass in a social prop file location");
		}
		File f = new File(this.socialPropFile);
		if (!f.exists()) {
			throw new IllegalArgumentException("File does not exist");
		}

		loadSocialProperties();
	}

	/**
	 * Loads the social properties from disk and refreshes derived cached values.
	 */
	public void loadSocialProperties() {
		this.socialData = Utility.loadProperties(this.socialPropFile);
		setLoginsAllowed();
		setAvailableProviders();
	}

	/**
	 * Builds the provider-login allowlist map from configured {@code *_login}
	 * properties plus the legacy registration flag.
	 */
	public void setLoginsAllowed() {
		this.loginsAllowedMap = new HashMap<>();
		// define the default provider set
		Set<String> defaultProviders = AuthProvider.getSocialPropKeys();

		// get all _login props
		Set<String> loginProps = socialData.stringPropertyNames().stream().filter(str -> str.endsWith("_login"))
				.collect(Collectors.toSet());
		for (String prop : loginProps) {
			// prop ex. ms_login
			// get provider from prop by split on _
			String provider = prop.split("_login")[0];

			this.loginsAllowedMap.put(provider, Boolean.parseBoolean(socialData.getProperty(prop)));
			// remove the provider from the defaultProvider list
			defaultProviders.remove(provider);
		}

		// for loop through the defaultProviders list to make sure we set the rest to
		// false
		for (String provider : defaultProviders) {
			this.loginsAllowedMap.put(provider, false);
		}

		// get if registration is allowed
		// TODO: delete this once FE pulls value from different location
		this.loginsAllowedMap.put("registration", isNativeRegistrationAllowed());
	}

	/**
	 * Determines whether native user registration is enabled.
	 *
	 * @return {@code true} when {@code native_registration} is enabled
	 */
	public boolean isNativeRegistrationAllowed() {
		return Boolean.parseBoolean(socialData.getProperty("native_registration") + "");
	}

	/**
	 * Builds the list of currently enabled login providers for client consumption.
	 */
	public void setAvailableProviders() {
		this.availableProviders = new ArrayList<Map<String, Object>>();

		// define the allProviders set
		Map<String, AuthProvider> allProviders = AuthProvider.getSocialPropKeysToEnum();

		// get all _login props
		Set<String> loginProps = socialData.stringPropertyNames().stream().filter(str -> str.endsWith("_login"))
				.collect(Collectors.toSet());
		for (String prop : loginProps) {
			// check if it allowed
			Boolean isAllowed = Boolean.parseBoolean(socialData.getProperty(prop));
			if (!isAllowed) {
				continue;
			}

			// get provider from prop by split on _
			String provider = prop.split("_login")[0];

			// get the name if it exists
			String name = allProviders.get(provider).getDisplayName();
			if (socialData.get(provider + "_display_name") != null) {
				String value = socialData.getProperty(provider + "_display_name");
				if (value != null && !(value = value.trim()).isEmpty()) {
					name = value;
				}
			}

			AuthProvider thisProvider = allProviders.get(provider);
			Map<String, Object> providerMap = new HashMap<>();
			providerMap.put("name", name);
			providerMap.put("provider", provider);
			providerMap.put("isOauth", thisProvider.isOAuth());
			providerMap.put("label", thisProvider.getLabel());
			this.availableProviders.add(providerMap);
		}
	}

	/**
	 * Checks whether access-key authentication is allowed for a provider.
	 *
	 * @param provider the authentication provider to evaluate
	 * @return {@code true} when access keys are permitted
	 */
	public boolean accessKeyAllowed(AuthProvider provider) {
		String prefix = provider.toString().toLowerCase();
		boolean accessKeysAllowed = Boolean
				.parseBoolean(SocialPropertiesUtil.getInstance().getProperty(prefix + "_access_keys_allowed") + "");
		// LEGACY
		if (!accessKeysAllowed && provider == AuthProvider.MICROSOFT) {
			accessKeysAllowed = Boolean
					.parseBoolean(SocialPropertiesUtil.getInstance().getProperty("ms_access_keys_allowed") + "");
		}

		return accessKeysAllowed;
	}

	/**
	 * Updates provider-scoped properties by prefixing each incoming key with the
	 * provider name.
	 *
	 * @param provider provider prefix (for example {@code google})
	 * @param mods     key/value updates without provider prefix
	 * @throws Exception if the update cannot be persisted
	 */
	public void updateProviderProperties(String provider, Map<String, String> mods) throws Exception {
		Map<String, String> updates = new HashMap<>(mods.size());
		for (String mod : mods.keySet()) {
			updates.put(provider + "_" + mod, mods.get(mod));
		}
		updateAllProperties(updates);
	}

	/**
	 * Updates and persists a set of social properties, then reloads in-memory
	 * caches.
	 *
	 * @param mods property key/value pairs to write
	 * @throws Exception if loading or saving the properties file fails
	 */
	public void updateAllProperties(Map<String, String> mods) throws Exception {
		Parameters params = new Parameters();
		FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(
				PropertiesConfiguration.class).configure(params.properties().setFile(new File(this.socialPropFile)));

		// Load
		FileBasedConfiguration config;
		try {
			config = builder.getConfiguration();
		} catch (Exception e1) {
			classLogger.error("Error loading PropertiesConfiguration for social properties file: {}",
					this.socialPropFile, e1);
			throw new IllegalArgumentException(
					"An unexpected error happened trying to access the properties. Please try again or reach out to server admin. Detailed message = "
							+ e1.getMessage(),
					e1);
		}

		// Modify
		for (String mod : mods.keySet()) {
			config.setProperty(mod, mods.get(mod));
		}

		// Save via the builder
		try {
			builder.save();
			reloadProps();
		} catch (Exception e1) {
			throw new IllegalArgumentException(
					"An unexpected error happened when saving the new login properties. Please try again or reach out to server admin. Detailed message = "
							+ e1.getMessage(),
					e1);
		}
	}

	/**
	 * Replaces the full contents of the social properties file and reloads caches.
	 * If writing fails, the previous file contents are restored when possible.
	 *
	 * @param newFileContents full replacement content for the file
	 * @throws IOException if reading, writing, or restoring the file fails
	 */
	public void updateAllProperties(String newFileContents) throws IOException {
		File currentSocialProperties = new File(this.socialPropFile);

		String currentContent = null;
		if (currentSocialProperties.exists()) {
			try {
				currentContent = new String(Files.readAllBytes(Paths.get(currentSocialProperties.toURI())));
			} catch (IOException e) {
				classLogger.error("Error reading social properties file: {}", this.socialPropFile, e);
				throw new IOException(
						"An error occurred reading the current social properties file. Detailed message = "
								+ e.getMessage());
			}
			currentSocialProperties.delete();
		}

		try {
			try (FileWriter fw = new FileWriter(currentSocialProperties, false)) {
				fw.write(newFileContents);
			}
			reloadProps();
		} catch (Exception e) {
			classLogger.error("Error writing new social properties file: {}", this.socialPropFile, e);
			// reset the values
			currentSocialProperties.delete();
			if (currentContent != null) {
				try (FileWriter fw = new FileWriter(currentSocialProperties, false)) {
					fw.write(currentContent);
				} catch (IOException e2) {
					classLogger.error("Error reverting social properties file to previous content: {}",
							this.socialPropFile, e2);
					throw new IOException(
							"A fatal error occurred and could not revert the social properties to an operational state. Detailed message = "
									+ e2.getMessage());
				}
				throw new IOException(
						"An error occurred writing the new social properties. Detailed message = " + e.getMessage());
			}
		}
	}

	/**
	 * Reads and returns the raw social properties file contents.
	 *
	 * @return entire social properties file as a string
	 * @throws NullPointerException if the file does not exist
	 * @throws IOException          if the file cannot be read
	 */
	public String getFileContents() throws NullPointerException, IOException {
		File currentSocialProperties = new File(this.socialPropFile);
		if (!currentSocialProperties.exists()) {
			throw new NullPointerException("Could not find the social properties file");
		}

		String currentContent = null;
		try {
			currentContent = new String(Files.readAllBytes(Paths.get(currentSocialProperties.toURI())));
		} catch (IOException e) {
			classLogger.error("Error reading social properties file: {}", this.socialPropFile, e);
			throw new IOException("An error occurred reading the current social properties file. Detailed message = "
					+ e.getMessage());
		}
		return currentContent;
	}

	/**
	 * Reloads social properties and clears all cached email sessions, stores, and
	 * derived property maps.
	 */
	public void reloadProps() {
		// null out values to be reset
		this.loadSocialProperties();
		closeSmtpEngine();
		closeMailStoreEngines();
		this.smtpEmailProps = null;
		this.smtpEmailStaticProps = null;

		this.pop3EmailProps = null;

		this.imapEmailProps = null;
	}

	/**
	 * Switch to using {@link #getAvailableProviders()}
	 *
	 * @return legacy provider-login allowlist map
	 */
	@Deprecated
	public Map<String, Boolean> getLoginsAllowed() {
		return this.loginsAllowedMap;
	}

	/**
	 * Returns the enabled provider metadata built from social properties.
	 *
	 * @return list of enabled provider metadata maps
	 */
	public List<Map<String, Object>> getAvailableProviders() {
		return this.availableProviders;
	}

	/**
	 * Returns a social property value.
	 *
	 * @param key property key
	 * @return property value or {@code null} when absent
	 */
	public String getProperty(String key) {
		return this.socialData.getProperty(key);
	}

	/**
	 * Returns a social property value with fallback.
	 *
	 * @param key          property key
	 * @param defaultValue fallback when key is absent
	 * @return configured value or {@code defaultValue}
	 */
	public String getProperty(String key, String defaultValue) {
		return this.socialData.getProperty(key, defaultValue);
	}

	/**
	 * Returns a raw social property object.
	 *
	 * @param key property key
	 * @return raw property value object or {@code null} when absent
	 */
	public Object get(Object key) {
		return this.socialData.get(key);
	}

	/**
	 * Checks whether a social property key exists.
	 *
	 * @param key property key
	 * @return {@code true} when the key exists
	 */
	public boolean containsKey(String key) {
		return this.socialData.containsKey(key);
	}

	/**
	 * Returns all property keys currently loaded.
	 *
	 * @return set of property names
	 */
	public Set<String> stringPropertyNames() {
		return this.socialData.stringPropertyNames();
	}

	/**
	 * Returns the login redirect URL normalized to end in {@code #!/login}.
	 *
	 * @return normalized login redirect URL
	 */
	public String getLoginRedirect() {
		String redirectUrl = this.socialData.getProperty("redirect");
		if (redirectUrl.endsWith("#!/login")) {
			return redirectUrl;
		}
		// accounting for some user inputs
		if (!redirectUrl.endsWith("/")) {
			redirectUrl = redirectUrl + "/";
		}
		if (!redirectUrl.endsWith("#!/")) {
			redirectUrl = redirectUrl + "#!/";
		}
		return redirectUrl + "login";
	}

	/**
	 * Parses configured {@code saml_*} properties into a map of application keys to
	 * ordered source attribute lists.
	 *
	 * @return map of SAML application key to source attribute array
	 */
	public Map<String, String[]> getSamlAttributeNames() {
		final String NULL_INPUT = "NULL";

		String prefix = Constants.SAML + "_";
		Map<String, String[]> samlAttrMap = new HashMap<>();
		Set<String> samlProps = this.socialData.stringPropertyNames().stream().filter(str -> str.startsWith(prefix))
				.collect(Collectors.toSet());
		for (String samlKey : samlProps) {
			// key
			String socialKey = samlKey.replaceFirst(prefix, "").toLowerCase();
			// value
			if (socialData.get(samlKey) == null) {
				continue;
			}
			String socialValue = this.socialData.get(samlKey).toString().trim();
			if (socialValue.isEmpty() || socialValue.equals(NULL_INPUT)) {
				continue;
			}
			socialValue = socialValue.toLowerCase();

			String[] keyGeneratedBy = socialValue.split("\\+");
			samlAttrMap.putIfAbsent(socialKey, keyGeneratedBy);
		}
		return samlAttrMap;
	}

	/**
	 * Indicates whether central SMTP configuration is enabled.
	 *
	 * @return {@code true} when SMTP is enabled
	 */
	public boolean smtpEmailEnabled() {
		return Boolean.parseBoolean(this.socialData.getProperty(SMTP_ENABLED, "false"));
	}

	/**
	 * Indicates whether central POP3 configuration is enabled.
	 *
	 * @return {@code true} when POP3 is enabled
	 */
	public boolean pop3EmailEnabled() {
		return Boolean.parseBoolean(this.socialData.getProperty(POP3_ENABLED, "false"));
	}

	/**
	 * Indicates whether central IMAP configuration is enabled.
	 *
	 * @return {@code true} when IMAP is enabled
	 */
	public boolean imapEmailEnabled() {
		return Boolean.parseBoolean(this.socialData.getProperty(IMAP_ENABLED, "false"));
	}

	/**
	 * Loads SMTP transport properties from keys prefixed with {@code smtp_}.
	 *
	 * @return SMTP properties or {@code null} when none are configured
	 */
	public Properties loadSmtpEmailProperties() {
		final String prefix = "smtp_";
		Properties smtpProp = new Properties();
		Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str -> str.startsWith(prefix))
				.collect(Collectors.toSet());
		for (String key : smtpKeys) {
			if (SMTP_ONLY_CUSTOM_PROPS.equals(key)) {
				continue;
			}
			Object smtpValue = socialData.get(key);
			if (smtpValue == null) {
				continue;
			}
			// clean up key
			String smtpKey = key.replaceFirst(prefix, "");
			smtpProp.put(smtpKey, smtpValue);
		}
		if (smtpProp.isEmpty()) {
			return null;
		}
		return smtpProp;
	}

	/**
	 * Loads POP3 transport properties from keys prefixed with {@code pop3_}.
	 *
	 * @return POP3 properties or {@code null} when none are configured
	 */
	public Properties loadPop3EmailProperties() {
		final String prefix = "pop3_";
		Properties pop3Prop = new Properties();
		Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str -> str.startsWith(prefix))
				.collect(Collectors.toSet());
		for (String key : smtpKeys) {
			if (POP3_ONLY_CUSTOM_PROPS.equals(key)) {
				continue;
			}
			Object smtpValue = socialData.get(key);
			if (smtpValue == null) {
				continue;
			}
			// clean up key
			String smtpKey = key.replaceFirst(prefix, "");
			pop3Prop.put(smtpKey, smtpValue);
		}
		if (pop3Prop.isEmpty()) {
			return null;
		}
		return pop3Prop;
	}

	/**
	 * Loads IMAP transport properties from keys prefixed with {@code imap_}.
	 *
	 * @return IMAP properties or {@code null} when none are configured
	 */
	public Properties loadImapEmailProperties() {
		final String prefix = "imap_";
		Properties imapProp = new Properties();
		Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str -> str.startsWith(prefix))
				.collect(Collectors.toSet());
		for (String key : smtpKeys) {
			if (IMAP_ONLY_CUSTOM_PROPS.equals(key)) {
				continue;
			}
			Object smtpValue = socialData.get(key);
			if (smtpValue == null) {
				continue;
			}
			// clean up key
			String smtpKey = key.replaceFirst(prefix, "");
			imapProp.put(smtpKey, smtpValue);
		}
		if (imapProp.isEmpty()) {
			return null;
		}
		return imapProp;
	}

	/**
	 * Loads static email template replacement properties from keys prefixed with
	 * {@code smtpprop_}.
	 *
	 * @return map of static template replacement values
	 */
	public Map<String, String> loadSmtpEmailStaticProps() {
		final String prefix = "smtpprop_";
		Map<String, String> emailStaticProps = new HashMap<>();
		Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str -> str.startsWith(prefix))
				.collect(Collectors.toSet());
		for (String key : smtpKeys) {
			String smtpValue = socialData.getProperty(key);
			if (smtpValue == null) {
				continue;
			}
			// clean up key
			String smtpKey = key.replaceFirst(prefix, "");
			emailStaticProps.put(smtpKey, smtpValue);
		}
		return emailStaticProps;
	}

	/**
	 * Initializes the shared SMTP connection from configured SMTP properties.
	 * Security defaults are applied unless {@code smtp_only_custom_props=true}.
	 *
	 * <p>
	 * The connection itself is an {@link SMTPFunctionEngine} opened against these
	 * properties rather than a session built here, so the mail server gets the same
	 * TLS handling as any other mail connection.
	 *
	 * @throws IllegalArgumentException when SMTP is enabled but configuration is
	 *                                  missing or invalid
	 */
	public void loadSmtpEmailSession() {
		if (this.socialData == null || !smtpEmailEnabled()) {
			return;
		}
		if (this.smtpEmailProps == null || this.smtpEmailProps.isEmpty()) {
			this.smtpEmailProps = loadSmtpEmailProperties();
		}
		if (this.smtpEmailProps == null || this.smtpEmailProps.isEmpty()) {
			throw new IllegalArgumentException(
					"SMTP properties not defined for this instance but it is enabled. Please reach out to an admin to configure");
		}
		this.smtpEmailStaticProps = getSmtpEmailStaticProps();

		// social.properties already speaks in raw jakarta.mail keys, which the
		// engine takes as is, so only the credentials and the custom props flag
		// have to be translated onto the engine's own key names
		Properties engineProps = new Properties();
		engineProps.putAll(this.smtpEmailProps);
		engineProps.put(SMTPFunctionEngine.ONLY_CUSTOM_PROPS_KEY,
				this.socialData.getProperty(SMTP_ONLY_CUSTOM_PROPS, "false"));
		putIfPresent(engineProps, SMTPFunctionEngine.SMTP_USERNAME_KEY, getSmtpUsername());
		putIfPresent(engineProps, SMTPFunctionEngine.SMTP_PASSWORD_KEY, getSmtpPassword());
		// smtp_sender is deliberately not passed along. callers read the sender off
		// getSmtpSender() rather than off the connection, and handing it to the
		// engine would make a malformed one fail the whole mail server at load

		try {
			classLogger.info("Making connection to the email server");
			this.smtpEngine = SMTPFunctionEngine.openTransientEngine(SMTP_ENGINE_ID, engineProps);
		} catch (Exception e) {
			classLogger.error("Error creating SMTP email session", e);
			throw new IllegalArgumentException(
					"Error occurred connecting to the email session defined. Please ensure the proper settings are set for connecting. Detailed error: "
							+ e.getMessage(),
					e);
		}
	}

	/**
	 * Copies a value onto the engine properties only when there is one, so an unset
	 * social property does not arrive at the engine as a blank string.
	 *
	 * @param props the engine properties being built
	 * @param key   the engine key to set
	 * @param value the configured value, possibly null or blank
	 */
	private static void putIfPresent(Properties props, String key, String value) {
		if (value != null && !value.trim().isEmpty()) {
			props.put(key, value);
		}
	}

	/**
	 * Initializes the shared POP3 connection from configured POP3 properties.
	 * Security defaults are applied unless {@code pop3_only_custom_props=true}.
	 *
	 * <p>
	 * The connection is a {@link POP3FunctionEngine} opened against these
	 * properties rather than a store connected here, so the mailbox gets the same
	 * TLS handling as any other mail connection. The engine connects on first use
	 * and reopens the connection after the mail server has dropped an idle one.
	 *
	 * @throws IllegalArgumentException when POP3 is enabled but configuration is
	 *                                  missing or invalid
	 */
	public void loadPop3EmailSession() {
		if (this.socialData == null || !pop3EmailEnabled()) {
			return;
		}
		if (this.pop3EmailProps == null || this.pop3EmailProps.isEmpty()) {
			this.pop3EmailProps = loadPop3EmailProperties();
		}
		if (this.pop3EmailProps == null || this.pop3EmailProps.isEmpty()) {
			throw new IllegalArgumentException(
					"POP3 properties not defined for this instance but it is enabled. Please reach out to an admin to configure");
		}

		// social.properties already speaks in raw jakarta.mail keys, which the
		// engine takes as is, so only the credentials and the custom props flag
		// have to be translated onto the engine's own key names
		Properties engineProps = new Properties();
		engineProps.putAll(this.pop3EmailProps);
		engineProps.put(POP3FunctionEngine.ONLY_CUSTOM_PROPS_KEY,
				this.socialData.getProperty(POP3_ONLY_CUSTOM_PROPS, "false"));
		putIfPresent(engineProps, POP3FunctionEngine.POP3_USERNAME_KEY, getPop3Username());
		putIfPresent(engineProps, POP3FunctionEngine.POP3_PASSWORD_KEY, getPop3Password());

		try {
			classLogger.info("Opening the connection to the pop3 mail server");
			this.pop3Engine = POP3FunctionEngine.openTransientEngine(POP3_ENGINE_ID, engineProps);
		} catch (Exception e) {
			classLogger.error("Error creating the POP3 connection", e);
			throw new IllegalArgumentException(
					"Error occurred establishing the pop3 connection. Please ensure the proper settings are set for connecting. Detailed error: "
							+ e.getMessage(),
					e);
		}
	}

	/**
	 * Initializes the shared IMAP connection from configured IMAP properties.
	 * Security defaults are applied unless {@code imap_only_custom_props=true}.
	 *
	 * <p>
	 * The connection is an {@link IMAPFunctionEngine} opened against these
	 * properties rather than a store connected here, and it connects on first use
	 * rather than at load.
	 *
	 * @throws IllegalArgumentException when IMAP is enabled but configuration is
	 *                                  missing or invalid
	 */
	public void loadImapEmailSession() {
		if (this.socialData == null || !imapEmailEnabled()) {
			return;
		}
		if (this.imapEmailProps == null || this.imapEmailProps.isEmpty()) {
			this.imapEmailProps = loadImapEmailProperties();
		}
		if (this.imapEmailProps == null || this.imapEmailProps.isEmpty()) {
			throw new IllegalArgumentException(
					"IMAP properties not defined for this instance but it is enabled. Please reach out to an admin to configure");
		}

		Properties engineProps = new Properties();
		engineProps.putAll(this.imapEmailProps);
		engineProps.put(IMAPFunctionEngine.ONLY_CUSTOM_PROPS_KEY,
				this.socialData.getProperty(IMAP_ONLY_CUSTOM_PROPS, "false"));
		putIfPresent(engineProps, IMAPFunctionEngine.IMAP_USERNAME_KEY, getImapUsername());
		putIfPresent(engineProps, IMAPFunctionEngine.IMAP_PASSWORD_KEY, getImapPassword());

		try {
			classLogger.info("Opening the connection to the imap mail server");
			this.imapEngine = IMAPFunctionEngine.openTransientEngine(IMAP_ENGINE_ID, engineProps);
		} catch (Exception e) {
			classLogger.error("Error creating the IMAP connection", e);
			throw new IllegalArgumentException(
					"Error occurred establishing the imap connection. Please ensure the proper settings are set for connecting. Detailed error: "
							+ e.getMessage(),
					e);
		}
	}

	/**
	 * Returns the configured SMTP username.
	 *
	 * @return SMTP username or {@code null} when not set
	 */
	public String getSmtpUsername() {
		return this.socialData.getProperty(SMTP_USERNAME);
	}

	/**
	 * Returns the configured SMTP password.
	 *
	 * @return SMTP password or {@code null} when not set
	 */
	public String getSmtpPassword() {
		return this.socialData.getProperty(SMTP_PASSWORD);
	}

	/**
	 * Returns the configured SMTP sender address.
	 *
	 * @return sender email address or {@code null} when not set
	 */
	public String getSmtpSender() {
		return this.socialData.getProperty(SMTP_SENDER);
	}

	/**
	 * Returns the configured POP3 username.
	 *
	 * @return POP3 username or {@code null} when not set
	 */
	public String getPop3Username() {
		return this.socialData.getProperty(POP3_USERNAME);
	}

	/**
	 * Returns the configured POP3 password.
	 *
	 * @return POP3 password or {@code null} when not set
	 */
	public String getPop3Password() {
		return this.socialData.getProperty(POP3_PASSWORD);
	}

	/**
	 * Returns the configured IMAP username.
	 *
	 * @return IMAP username or {@code null} when not set
	 */
	public String getImapUsername() {
		return this.socialData.getProperty(IMAP_USERNAME);
	}

	/**
	 * Returns the configured IMAP password.
	 *
	 * @return IMAP password or {@code null} when not set
	 */
	public String getImapPassword() {
		return this.socialData.getProperty(IMAP_PASSWORD);
	}

	/**
	 * Returns the cached SMTP connection, loading it if necessary.
	 *
	 * @return the SMTP engine or {@code null} when SMTP is disabled
	 */
	public SMTPFunctionEngine getSmtpEngine() {
		if (this.smtpEngine == null) {
			loadSmtpEmailSession();
		}
		return this.smtpEngine;
	}

	/**
	 * Drops the cached SMTP connection so the next caller rebuilds it from the
	 * current properties.
	 */
	private void closeSmtpEngine() {
		if (this.smtpEngine == null) {
			return;
		}
		try {
			this.smtpEngine.close();
		} catch (IOException e) {
			classLogger.warn("Error closing the smtp connection", e);
		}
		this.smtpEngine = null;
	}

	/**
	 * Returns the cached POP3 connection, loading it if necessary.
	 *
	 * @return the POP3 engine or {@code null} when POP3 is disabled
	 */
	public POP3FunctionEngine getPop3Engine() {
		if (this.pop3Engine == null) {
			loadPop3EmailSession();
		}
		return this.pop3Engine;
	}

	/**
	 * Returns the cached IMAP connection, loading it if necessary.
	 *
	 * @return the IMAP engine or {@code null} when IMAP is disabled
	 */
	public IMAPFunctionEngine getImapEngine() {
		if (this.imapEngine == null) {
			loadImapEmailSession();
		}
		return this.imapEngine;
	}

	/**
	 * Drops the cached POP3 and IMAP connections so the next caller rebuilds them
	 * from the current properties.
	 */
	private void closeMailStoreEngines() {
		if (this.pop3Engine != null) {
			try {
				this.pop3Engine.close();
			} catch (IOException e) {
				classLogger.warn("Error closing the pop3 connection", e);
			}
			this.pop3Engine = null;
		}
		if (this.imapEngine != null) {
			try {
				this.imapEngine.close();
			} catch (IOException e) {
				classLogger.warn("Error closing the imap connection", e);
			}
			this.imapEngine = null;
		}
	}

	/**
	 * Returns a defensive copy of configured SMTP properties.
	 *
	 * @return SMTP properties copy or {@code null} when not configured
	 */
	public Properties getSmtpEmailProps() {
		if (this.smtpEmailProps == null) {
			this.smtpEmailProps = loadSmtpEmailProperties();
		}
		if (this.smtpEmailProps == null) {
			return null;
		}
		return new Properties(this.smtpEmailProps);
	}

	/**
	 * Returns a defensive copy of configured POP3 properties.
	 *
	 * @return POP3 properties copy or {@code null} when not configured
	 */
	public Properties getPop3EmailProps() {
		if (this.pop3EmailProps == null) {
			this.pop3EmailProps = loadPop3EmailProperties();
		}
		if (this.pop3EmailProps == null) {
			return null;
		}
		return new Properties(this.pop3EmailProps);
	}

	/**
	 * Returns a defensive copy of configured IMAP properties.
	 *
	 * @return IMAP properties copy or {@code null} when not configured
	 */
	public Properties getImapEmailProps() {
		if (this.imapEmailProps == null) {
			this.imapEmailProps = loadImapEmailProperties();
		}
		if (this.imapEmailProps == null) {
			return null;
		}
		return new Properties(this.imapEmailProps);
	}

	/**
	 * Returns a defensive copy of static SMTP email template properties.
	 *
	 * @return static SMTP template properties copy or {@code null} when not
	 *         configured
	 */
	public Map<String, String> getSmtpEmailStaticProps() {
		if (this.smtpEmailStaticProps == null) {
			this.smtpEmailStaticProps = loadSmtpEmailStaticProps();
		}
		if (this.smtpEmailStaticProps == null) {
			return null;
		}
		return new HashMap<>(this.smtpEmailStaticProps);
	}

}
