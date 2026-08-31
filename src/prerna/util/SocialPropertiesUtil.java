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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Session;
import jakarta.mail.Store;
import prerna.auth.AuthProvider;
import prerna.engine.impl.function.IMAPFunctionEngine;
import prerna.engine.impl.function.POP3FunctionEngine;
import prerna.engine.impl.function.SMTPFunctionEngine;
import prerna.util.ldap.ILdapAuthenticator;
import prerna.util.ldap.LdapAuthenticationFactory;

public final class SocialPropertiesUtil {

	private static final Logger classLogger = LogManager.getLogger(SocialPropertiesUtil.class);

	private static volatile SocialPropertiesUtil instance = null;
	private static volatile SocialPropertiesProcessor processor = null;
	private static String socialPropFile = null;

	/**
	 * Creates a SocialPropertiesUtil instance.
	 */
	public SocialPropertiesUtil() {
		SocialPropertiesUtil.socialPropFile = Utility.getDIHelperProperty(Constants.SOCIAL);
		if (SocialPropertiesUtil.socialPropFile != null) {
			File f = new File(SocialPropertiesUtil.socialPropFile);
			if (!f.exists()) {
				classLogger.warn("No social.properties file found!");
				classLogger.warn("No social.properties file found!");
				classLogger.warn("No social.properties file found!");
			} else {
				SocialPropertiesUtil.processor = new SocialPropertiesProcessor(SocialPropertiesUtil.socialPropFile);
			}
		} else {
			classLogger.warn("No social.properties defined in RDF_Map.prop!");
			classLogger.warn("No social.properties defined in RDF_Map.prop!");
			classLogger.warn("No social.properties defined in RDF_Map.prop!");
		}
	}

	/**
	 * Returns the singleton instance.
	 */
	public static SocialPropertiesUtil getInstance() {
		if (instance != null) {
			return instance;
		}

		synchronized (SocialPropertiesUtil.class) {
			if (instance == null) {
				instance = new SocialPropertiesUtil();
			}
		}

		return instance;
	}

	/**
	 * Updates provider-scoped social properties.
	 *
	 * @param provider provider prefix
	 * @param mods     property updates
	 * @throws Exception if the update fails
	 */
	public void updateSocialProperties(String provider, Map<String, String> mods) throws Exception {
		SocialPropertiesUtil.processor.updateProviderProperties(provider, mods);
	}

	/**
	 * Replaces the full social properties file contents.
	 *
	 * @param newFileContents full file content
	 * @throws IOException if writing fails
	 */
	public void updateAllProperties(String newFileContents) throws IOException {
		SocialPropertiesUtil.processor.updateAllProperties(newFileContents);
	}

	/**
	 * Reads and returns the social properties file contents.
	 *
	 * @return full social properties file content
	 * @throws NullPointerException if the file is missing
	 * @throws IOException          if reading fails
	 */
	public String getFileContents() throws NullPointerException, IOException {
		return SocialPropertiesUtil.processor.getFileContents();
	}

	/**
	 * Switch to using {@link #getAvailableProviders()}
	 * 
	 * @param provider
	 * @return
	 */
	@Deprecated
	public Map<String, Boolean> getLoginsAllowed() {
		return SocialPropertiesUtil.processor.getLoginsAllowed();
	}

	/**
	 * Handles the accessKeysAllowed operation.
	 */
	public boolean accessKeysAllowed(AuthProvider provider) {
		return SocialPropertiesUtil.processor.accessKeyAllowed(provider);
	}

	/**
	 * Handles the isNativeRegistrationAllowed operation.
	 */
	public boolean isNativeRegistrationAllowed() {
		return SocialPropertiesUtil.processor.isNativeRegistrationAllowed();
	}

	/**
	 * Returns a list of available authentication providers, where each entry is a
	 * map containing the following keys:
	 *
	 * <ul>
	 * <li>{@code name} - the display name of the provider (e.g. "Google",
	 * "Facebook")</li>
	 * <li>{@code provider} - the provider key in social.properties</li>
	 * <li>{@code isOauth} - {@code true} if the provider uses OAuth</li>
	 * <li>{@code label} - the label associated with the provider enum</li>
	 * </ul>
	 *
	 * @return a {@link List} of {@link Map} objects, each representing one
	 *         available provider for logining into the system
	 */
	public List<Map<String, Object>> getAvailableProviders() {
		return SocialPropertiesUtil.processor.getAvailableProviders();
	}

	/**
	 * Handles the getProperty operation.
	 */
	public String getProperty(String key) {
		return SocialPropertiesUtil.processor.getProperty(key);
	}

	/**
	 * Handles the getProperty operation.
	 */
	public String getProperty(String key, String defaultValue) {
		return SocialPropertiesUtil.processor.getProperty(key, defaultValue);
	}

	/**
	 * Handles the get operation.
	 */
	public Object get(Object key) {
		return SocialPropertiesUtil.processor.get(key);
	}

	/**
	 * Handles the containsKey operation.
	 */
	public boolean containsKey(String key) {
		return SocialPropertiesUtil.processor.containsKey(key);
	}

	/**
	 * Handles the stringPropertyNames operation.
	 */
	public Set<String> stringPropertyNames() {
		return SocialPropertiesUtil.processor.stringPropertyNames();
	}

	/**
	 * Handles the getLoginRedirect operation.
	 */
	public String getLoginRedirect() {
		return SocialPropertiesUtil.processor.getLoginRedirect();
	}

	/**
	 * Handles the getSamlAttributeNames operation.
	 */
	public Map<String, String[]> getSamlAttributeNames() {
		return SocialPropertiesUtil.processor.getSamlAttributeNames();
	}

	/**
	 * Handles the emailEnabled operation.
	 */
	@Deprecated
	public boolean emailEnabled() {
		classLogger.warn("METHOD DEPRECATED - PLEASE USE smtpEmailEnabled()");
		return smtpEmailEnabled();
	}

	/**
	 * Handles the smtpEmailEnabled operation.
	 */
	public boolean smtpEmailEnabled() {
		return SocialPropertiesUtil.processor.smtpEmailEnabled();
	}

	/**
	 * Handles the getSmtpUsername operation.
	 */
	public String getSmtpUsername() {
		return SocialPropertiesUtil.processor.getSmtpUsername();
	}

	/**
	 * Handles the getSmtpPassword operation.
	 */
	public String getSmtpPassword() {
		return SocialPropertiesUtil.processor.getSmtpPassword();
	}

	/**
	 * Handles the getSmtpSender operation.
	 */
	public String getSmtpSender() {
		return SocialPropertiesUtil.processor.getSmtpSender();
	}

	/**
	 * The connection to the instance wide mail server, for a caller that sends
	 * through it rather than holding onto its {@link Session}.
	 *
	 * @return the smtp connection, or null when smtp is not enabled
	 */
	public SMTPFunctionEngine getSmtpEngine() {
		return SocialPropertiesUtil.processor.getSmtpEngine();
	}

	/**
	 * Handles the getEmailProps operation.
	 */
	public Properties getEmailProps() {
		return SocialPropertiesUtil.processor.getSmtpEmailProps();
	}

	/**
	 * Handles the getEmailStaticProps operation.
	 */
	public Map<String, String> getEmailStaticProps() {
		return SocialPropertiesUtil.processor.getSmtpEmailStaticProps();
	}

	/**
	 * Handles the pop3EmailEnabled operation.
	 */
	public boolean pop3EmailEnabled() {
		return SocialPropertiesUtil.processor.pop3EmailEnabled();
	}

	/**
	 * Handles the getPop3Username operation.
	 */
	public String getPop3Username() {
		return SocialPropertiesUtil.processor.getPop3Username();
	}

	/**
	 * Handles the getPop3Password operation.
	 */
	public String getPop3Password() {
		return SocialPropertiesUtil.processor.getPop3Password();
	}

	/**
	 * The connection to the instance wide POP3 mailbox, for a caller that reads
	 * through it rather than working the {@link Store} itself.
	 *
	 * @return the pop3 connection, or null when pop3 is not enabled
	 */
	public POP3FunctionEngine getPop3Engine() {
		return SocialPropertiesUtil.processor.getPop3Engine();
	}

	/**
	 * Handles the getPop3EmailProps operation.
	 */
	public Properties getPop3EmailProps() {
		return SocialPropertiesUtil.processor.getPop3EmailProps();
	}

	/**
	 * Handles the imapEmailEnabled operation.
	 */
	public boolean imapEmailEnabled() {
		return SocialPropertiesUtil.processor.imapEmailEnabled();
	}

	/**
	 * Handles the getImapUsername operation.
	 */
	public String getImapUsername() {
		return SocialPropertiesUtil.processor.getImapUsername();
	}

	/**
	 * Handles the getImapPassword operation.
	 */
	public String getImapPassword() {
		return SocialPropertiesUtil.processor.getImapPassword();
	}

	/**
	 * The connection to the instance wide IMAP mailbox, for a caller that reads
	 * through it rather than working the {@link Store} itself.
	 *
	 * @return the imap connection, or null when imap is not enabled
	 */
	public IMAPFunctionEngine getImapEngine() {
		return SocialPropertiesUtil.processor.getImapEngine();
	}

	/**
	 * Handles the getImapEmailProps operation.
	 */
	public Properties getImapEmailProps() {
		return SocialPropertiesUtil.processor.getImapEmailProps();
	}

	/**
	 * Handles the reloadProps operation.
	 */
	public void reloadProps() {
		SocialPropertiesUtil.processor.reloadProps();
	}

	/**
	 * Creates and loads the configured LDAP authenticator.
	 *
	 * @return loaded LDAP authenticator
	 * @throws IOException if authenticator initialization fails
	 */
	public ILdapAuthenticator getLdapAuthenticator() throws IOException {
		String ldapType = SocialPropertiesUtil.processor.getProperty(ILdapAuthenticator.LDAP_TYPE);
		ILdapAuthenticator ldapAuthenticator = LdapAuthenticationFactory.getAuthenticator(ldapType);
		ldapAuthenticator.load();
		return ldapAuthenticator;
	}

}
